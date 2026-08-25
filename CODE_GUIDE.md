# 代码职责说明（当前完整版本）

## 启动与配置

- `SeckillApplication.java`：启动类、开启定时任务。
- `SeckillProperties.java`：支付超时、lease、对账宽限、调度参数、Topic/Group。
- `application.yml`：MySQL、Redis、RocketMQ 与业务参数。

## 数据库

- `schema.sql`：订单表、创建 guard、Outbox、补偿表。
- `OrderRepository.java`：订单查询、创建、支付/关单 CAS、`stock_released` 确认。
- `OrderCreateGuardRepository.java`：`lockOrCreate()` + `FOR UPDATE`，对同一个 `orderNo` 建立 DB 互斥决策区。
- `OutboxRepository.java`：Outbox 状态、抢占、重试、幂等业务键。
- `CompensationRepository.java`：Redis 补偿任务持久化与重试。

## Redis 与 Lua

- `RedisKeys.java`：库存、buyers、reservation、对账 ZSET、Producer 发送重试 ZSET。
- `reserve.lua`：原子扣库存、防重复购买、创建 `RESERVED`，同时写两个 pending ZSET。
- `claim_creating.lua`：`RESERVED -> CREATING`；`CREATING` lease 过期后允许新消费尝试重新 claim。
- `mark_ordered.lua`：
  - `CREATING -> ORDERED`；
  - `ORDERED` 幂等；
  - `RELEASED` 返回特殊结果，由 Java 重查 DB；
  - `RESERVED` 非法。
- `release_unordered.lua`：只能在 guard=`ABORTED` 后释放 `RESERVED/CREATING`；遇到 `ORDERED` 拒绝。
- `release_canceled.lua`：DB 已 `CANCELED` 后释放 `ORDERED/CREATING`；`RELEASED` 幂等；`RESERVED` 非法。
- `MarkOrderedResult.java`：`markOrdered` 的显式状态结果。
- `ReleaseResult.java`：库存释放 Lua 的结果枚举。
- `RedisReservationService.java`：所有 Redis Lua 的 Java 封装与两个 ZSET 的扫描/调度。

## 秒杀入口与 CREATE_ORDER Producer

- `SeckillEntryService.java`：Redis 预占后尝试同步发送 CREATE_ORDER；非 `SEND_OK`/异常不回补库存。
- `CreateOrderMessageSender.java`：统一发送 CREATE_ORDER。
- `CreateOrderSendRetryScheduler.java`：Producer 侧可靠重试。
- `SeckillController.java`：秒杀 HTTP 入口。
- `DebugController.java`：Demo 库存初始化/查询。

## 创建订单

- `CreateOrderConsumer.java`：
  1. 先查 DB 是否已有订单；
  2. Redis claim `CREATING`；
  3. 调用 `OrderTxService#createOrderAndCloseOutbox()` 竞争 DB guard；
  4. DB 异常后重新查 DB，不根据异常本身猜是否提交；
  5. guard=`ABORTED` 时走统一安全释放/冲突检查；
  6. `CREATED_NOW` 后调用 `OrderConsistencyService#markOrderedAfterConfirmedOrder()`。

- `OrderTxService.java`：
  - 创建事务：guard + order + CLOSE_ORDER outbox + `CREATED`；
  - 关单事务：`WAIT_PAY -> CANCELED` + RELEASE_STOCK outbox；
  - ABORT fence：对账/最终失败方竞争 guard。
- `CreateOrderTxResult.java`：`CREATED_NOW / ALREADY_CREATED / ABORTED_BY_RECONCILIATION`。

## 统一一致性处理

- `OrderConsistencyService.java`：DB -> Redis 的统一入口。
  - `WAIT_PAY/PAID`：严格 `markOrdered`。
  - `CANCELED`：`releaseCanceled()` + `stock_released`。
  - `markOrdered` 遇到 `RELEASED`：重查 DB；最新为 `CANCELED` 则接受 `RELEASED` 并补 DB 确认位，最新为 `WAIT_PAY/PAID` 则报严重异常。
  - guard=`ABORTED` + Redis=`ORDERED`：重查 DB，不自动把 ORDERED 当脏数据释放。

## 对账、DLQ、补偿

- `ReservationReconcileScheduler.java`：`firstCheckAt` 到期后扫描 reservation；可以再次检查 lease 减少与健康消费者竞争，但最终 correctness 由 DB guard 保证。
- `CreateOrderDlqConsumer.java`：CREATE_ORDER 重试耗尽入口。
- `FinalCreateFailureService.java`：DLQ 最终事实确认；先竞争 ABORT fence，再安全释放。
- `CompensationScheduler.java`：Redis 暂时失败后的持久化补偿重试，每次仍重新确认 DB/guard。

## Outbox 与关单

- `OutboxPublisherScheduler.java`：`NEW/RETRY -> SENDING -> SENT/RETRY/DEAD`；检查 RocketMQ `SendStatus`，并恢复卡死 `SENDING`。
- `CloseOrderConsumer.java`：消费延时关单消息，DB CAS 只有 `WAIT_PAY` 才能变 `CANCELED`。
- `ExpiredOrderScanner.java`：根据 DB `expire_time` 兜底关单。
- `ReleaseStockConsumer.java`：订单已 `CANCELED` 后消费 RELEASE_STOCK 并调用幂等 Lua。
- `CanceledOrderReleaseReconciler.java`：扫描 `CANCELED + stock_released=0`，处理“Redis 已释放但 DB 确认位没写成功”等窗口。

## 支付

- `PaymentController.java`：Demo 支付入口。
- `OrderTxService#pay()`：`WAIT_PAY -> PAID` CAS，与关单竞争。

## 设计边界

- `firstCheckAt`：避免 reservation 过早进入对账。
- `leaseUntil`：减少重复消费者/对账竞争，不是最终互斥保证。
- MySQL guard：创建 vs ABORT 的最终 correctness 边界。
- Outbox：DB 与 MQ 之间的可靠事件桥梁。
- Redis Lua + `RELEASED`：库存释放的幂等边界。
- DB 是订单业务事实源；跨存储状态冲突时优先重新查询 DB，不凭旧 Redis/旧 DB 快照猜测。
