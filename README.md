# 秒杀最终一致性 Demo（当前完整版本）

技术栈：Java 17、Spring Boot、MySQL 8、Redis、RocketMQ 5.x。

## 1. 核心原则

- Redis 负责高并发库存预占与 reservation 状态机。
- MySQL `seckill_order_create_guard` 是“创建订单”与“永久放弃创建”之间的最终安全栅栏。
- `order + CLOSE_ORDER outbox + guard=CREATED` 在同一个 DB 事务提交。
- `WAIT_PAY -> CANCELED + RELEASE_STOCK outbox` 在同一个 DB 事务提交。
- 所有 Redis 库存释放都经过 Lua，禁止业务代码直接 `INCR stock`。
- Redis/MySQL 可以短暂不一致，但所有关键动作都必须幂等、可重试、有最终兜底。
- 状态不确定时宁可暂时占住库存，也不贸然回补造成超卖。

## 2. Redis reservation

```text
RESERVED -> CREATING -> ORDERED -> RELEASED
    |          |
    |          +---- guard=ABORTED -> RELEASED
    +--------------- guard=ABORTED -> RELEASED
```

- `RESERVED`：库存已经预占，DB 订单尚未进入创建阶段。
- `CREATING`：消费者正在/曾经尝试创建订单；`leaseUntil` 只用于减少重复消费者同时冲 DB。
- `ORDERED`：DB 已经存在合法订单。当前系统把 `ORDERED => DB order exists` 作为强不变量。
- `RELEASED`：这次预占对应的库存已经幂等回补。

当前版本不使用 `ownerToken`，也没有 `rollbackCreating()`。

## 3. firstCheckAt 与 leaseUntil

- `firstCheckAt`：reservation 最早什么时候允许进入对账候选集合。
- `leaseUntil`：`CREATING` 多久以后允许新的消费者重新尝试 claim。
- 两者都只是降低无意义竞争；真正解决消费者/对账并发正确性的是 MySQL guard。

## 4. Producer 可靠发送 CREATE_ORDER

`reserve.lua` 原子完成：

- `stock - 1`
- `SADD buyer`
- 创建 `reservation=RESERVED`
- 写入 reservation 对账 ZSET
- 写入 Producer 发送重试 ZSET

首次同步发送：

- `SEND_OK`：删除 Producer 发送重试记录。
- 非 `SEND_OK` / 异常：不能立即回补库存，因为 Broker 可能已经收到消息；保留待发送记录。

`CreateOrderSendRetryScheduler` 后续继续发送。重复发送由消费者幂等与 DB guard 处理。

## 5. MySQL guard

`seckill_order_create_guard` 状态：

```text
CREATING
   |\
   | +---- CREATED
   |
   +------ ABORTED
```

消费者和对账/DLQ/补偿都必须竞争同一个 `order_no` guard。

- 创建方先成功：`CREATED`，未成单释放路径不能再回补。
- 对账/最终失败方先成功：`ABORTED`，迟到或复活消费者不能再创建该订单。
- guard 锁一直持有到事务 `COMMIT/ROLLBACK`，因此既能处理消费者之间竞争，也能处理消费者与对账之间竞争。

## 6. 创建订单事务

`OrderTxService#createOrderAndCloseOutbox()`：

```text
BEGIN
lock/create guard
INSERT order(WAIT_PAY)
INSERT outbox(CLOSE_ORDER)
guard -> CREATED
COMMIT
```

因此当前正常业务路径维护不变量：

```text
DB order exists
=> 对应 CLOSE_ORDER outbox 已与订单同事务写入（对于创建时为 WAIT_PAY 的订单）
```

后续发现已有 `WAIT_PAY` 订单时，不再重复检查/补写 CLOSE_ORDER outbox；这里只按 DB 事实同步 Redis。

## 7. markOrdered 的严格状态规则

`mark_ordered.lua` 只做以下处理：

- `CREATING -> ORDERED`：正常推进。
- `ORDERED`：幂等成功。
- `RELEASED`：合法终态，不允许反向改成 `ORDERED`；返回给 Java 重新查询 DB 最新订单状态。
- `RESERVED`：当前协议下非法，因为 DB 创建订单前必须先 `RESERVED -> CREATING`。
- 其他状态：非法。

Java 如果在 `markOrdered` 时看到 `RELEASED`：

- DB 最新状态为 `CANCELED`：说明并发关单已经完成，`RELEASED` 正确，只补 `stock_released=1`。
- DB 最新状态为 `WAIT_PAY/PAID`：活动订单库存却已释放，属于严重一致性异常。

## 8. 创建订单异常后的处理

`createOrderAndCloseOutbox()` 抛异常不代表事务一定没提交，例如 COMMIT 成功但响应丢失。

消费者重新查询 DB：

- DB 有订单：按订单最新状态同步 Redis。
  - `WAIT_PAY/PAID`：走严格 `markOrdered` 逻辑。
  - `CANCELED`：走 `releaseCanceled()`。
- DB 明确没订单：保持 Redis `CREATING`，抛异常让 MQ 重试；不回滚为 `RESERVED`。
- DB 查询本身失败：状态未知，不释放库存。

## 9. releaseCanceled 的状态规则

`release_canceled.lua` 只能在 DB 已明确存在订单且状态为 `CANCELED` 后调用：

- `ORDERED -> RELEASED`：正常取消回补。
- `CREATING -> RELEASED`：允许异常自愈；表示 DB 已经成单并取消，但 Redis `markOrdered` 落后。
- `RELEASED`：幂等返回。
- `RESERVED`：当前协议下非法。
- 其他状态：非法。

## 10. ABORTED + Redis ORDERED

普通 `release_unordered.lua` 只允许 guard 已经 `ABORTED` 后释放 `RESERVED/CREATING`；遇到 `ORDERED` 必须拒绝。

Java 再查 DB：

- DB 无订单：违反强不变量 `ORDERED => DB order exists`，不自动回补，抛异常/重试/告警。
- DB `CANCELED`：按 `releaseCanceled()` 处理，同时记录 guard 不一致。
- DB `WAIT_PAY/PAID`：真实活动订单仍占库存，绝不能释放，抛严重一致性异常。

## 11. Outbox

订单创建：

```text
order + CLOSE_ORDER outbox + guard=CREATED
```

订单关闭：

```text
WAIT_PAY -> CANCELED + RELEASE_STOCK outbox
```

Outbox 状态：

- `NEW`：待首次发送。
- `SENDING`：某个 Publisher 已抢到发送权。
- `RETRY`：上次发送失败，等待 `next_retry_at`。
- `SENT`：已确认发送到 RocketMQ。
- `DEAD`：达到正常重试上限。

卡死的 `SENDING` 会被恢复为 `RETRY`。`DEAD` 只表示 MQ 投递链路失败，不代表可以直接释放库存。

## 12. stock_released

`seckill_order.stock_released` 是 DB 侧“Redis 库存释放已确认”标志，不是 Redis 实时状态本身。

- `CANCELED + 0`：库存应该释放，但 DB 尚未确认 Redis release 已完成。
- `CANCELED + 1`：Redis 释放已确认。

可能出现 Redis 已经 `RELEASED`，但 `stock_released=0`（Redis 成功后 DB 标记前进程宕机）。
`CanceledOrderReleaseReconciler` 会重新调用幂等 `releaseCanceled()`：如果返回 `ALREADY_RELEASED`，只补 DB 确认位，不会重复 `stock+1`。

## 13. 启动

1. 执行 `src/main/resources/schema.sql`
2. 修改 `application.yml` 中 MySQL / Redis / RocketMQ 地址
3. 创建 Topic：
   - `seckill-create-order`
   - `seckill-close-order`
   - `seckill-release-stock`
4. 启动应用

初始化库存：

```bash
curl -X POST http://localhost:8080/debug/stock/1001/10
```

秒杀：

```bash
curl -X POST "http://localhost:8080/api/seckill/1001?userId=1"
```

支付：

```bash
curl -X POST http://localhost:8080/api/pay/{orderNo}
```

## 14. 最重要的不变量

1. `RELEASED` 最多真正回补一次库存。
2. guard=`ABORTED` 后任何创建路径都不能 INSERT 该 `orderNo`。
3. guard=`CREATED` 后未成单释放路径不能回补库存。
4. 所有 `seckill_order` 创建都必须经过 guard 协议。
5. `reservation=ORDERED => DB order exists`。
6. `WAIT_PAY/PAID` 真实订单存在时，绝不能因为 Redis/guard 异常直接释放库存。
7. `RELEASED` 不允许被 `markOrdered` 直接反向改回 `ORDERED`；必须先重查 DB 最新状态。
8. DB/Redis/MQ 状态不确定时不猜测。
