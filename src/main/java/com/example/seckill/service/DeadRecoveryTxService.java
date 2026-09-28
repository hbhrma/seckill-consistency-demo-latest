package com.example.seckill.service;

import com.example.seckill.domain.OrderStatus;
import com.example.seckill.domain.OutboxEventType;
import com.example.seckill.domain.OutboxMessage;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.ReleaseStockMessage;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.repository.OutboxRepository;
import com.example.seckill.util.Jsons;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * DEAD 补偿中必须原子完成的 MySQL 操作。
 */
@Service
public class DeadRecoveryTxService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final Jsons jsons;

    public DeadRecoveryTxService(OrderRepository orderRepository,
                                 OutboxRepository outboxRepository,
                                 Jsons jsons) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.jsons = jsons;
    }

    /**
     * CLOSE_ORDER 已经 DEAD，而且订单已经超过支付截止时间时：
     *
     * WAIT_PAY -> CANCELED
     *      +
     * INSERT RELEASE_STOCK outbox
     *      +
     * 原 CLOSE_ORDER DEAD -> RESOLVED
     *
     * 三件事放在同一个 MySQL 本地事务中。
     *
     * @return true  表示本事务真正完成 WAIT_PAY -> CANCELED；
     *         false 表示 CAS 没成功，调用方必须重新查询订单最新状态。
     */
    @Transactional
    public boolean closeExpiredOrderFromDead(long closeOrderDeadOutboxId,
                                             String orderNo) {
        int affected = orderRepository.tryCloseExpired(orderNo);
        if (affected != 1) {        // ！！！！！！！！！！！！！！注意，如果一个transactional注解的方
                                    // 法，有两个sql操作，假设在两个sql操作中间有一个return，那么方法返回后，
                                    // 第一个sql一般来说是生效了
            return false;
        }

        SeckillOrder canceledOrder = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "order disappeared after CANCELED CAS, orderNo=" + orderNo));

        ReleaseStockMessage releaseMessage = new ReleaseStockMessage(
                canceledOrder.orderNo(),
                canceledOrder.userId(),
                canceledOrder.goodsId()
        );

        /*
         * biz_key 唯一，因此即使其他关单路径已经创建过 RELEASE_STOCK，
         * insertIfAbsent 也只会做 no-op。
         */
        outboxRepository.insertIfAbsent(
                UUID.randomUUID().toString(),
                "RELEASE_STOCK:" + orderNo,
                OutboxEventType.RELEASE_STOCK,
                jsons.toJson(releaseMessage),
                LocalDateTime.now()
        );

        outboxRepository.resolveDead(
                closeOrderDeadOutboxId,
                "expired WAIT_PAY order canceled by DEAD recovery"
        );
        return true;
    }

    /**
     * Redis 已经明确返回 RELEASED_NOW / ALREADY_RELEASED 后，
     * 在一个 DB 事务中补齐 stock_released=1，并关闭当前 DEAD 记录。
     *
     * 即使这个 DB 事务失败也没关系：Redis release 是幂等的，
     * 下一轮再调用会得到 ALREADY_RELEASED，然后重新补 DB 标志。
     */
    /* CAS 更新“失败”通常只是影响行数为 0，并不是数据库异常；事务不会自动停止。
     *
     * 业务代码必须检查 affectedRows，
     * 只有CAS成功时才执行依赖它的后续 SQL。*/

    /*
    * 对于第七个，我的理解是，执行到confirmStockReleasedAndResolveDead，
    * 库存回补操作一定是进行了，因此对于更新markStockReleased的结果affected为0还是为1，
    * 其实不重要，因为核心不变量已经维护，也就是订单状态为CANCELED，库存已经回补。
    * 对于resolveDead的更新而言，是affected=1最好，但如果是0，那么release_stock消息的状态可能是retry，sending，sent，
    * 但是还是一样的，核心不变量或者说结果已经达到，而且操作具有幂等性，即使release_stock的消费者消费消息，也不会出现打破核心不变量的情况。
    */

    @Transactional
    public void confirmStockReleasedAndResolveDead(long deadOutboxId,
                                                   String orderNo) {
        orderRepository.markStockReleased(orderNo);
        outboxRepository.resolveDead(
                deadOutboxId,
                "Redis stock release confirmed"
        );
    }

    @Transactional
    public void insertReleaseStockAndResolve(OutboxMessage outbox, String orderNo) {
        SeckillOrder canceledOrder = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "canceled order disappeared, orderNo=" + orderNo));

        if (canceledOrder.status() != OrderStatus.CANCELED) {
            throw new IllegalStateException(
                    "order is not CANCELED, orderNo="
                            + orderNo + ", status=" + canceledOrder.status()
            );
        }

        ReleaseStockMessage releaseMessage = new ReleaseStockMessage(
                canceledOrder.orderNo(),
                canceledOrder.userId(),
                canceledOrder.goodsId()
        );

        outboxRepository.insertIfAbsent(
                UUID.randomUUID().toString(),
                "RELEASE_STOCK:" + orderNo,
                OutboxEventType.RELEASE_STOCK,
                jsons.toJson(releaseMessage),
                LocalDateTime.now()
        );

        outboxRepository.resolveDead(
                outbox.id(),
                "insert release stock message record for canceled order, orderNo=" + orderNo
        );
    }
}
