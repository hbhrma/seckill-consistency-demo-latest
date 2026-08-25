package com.example.seckill.service;

import com.example.seckill.domain.OutboxEventType;
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
    @Transactional
    public void confirmStockReleasedAndResolveDead(long deadOutboxId,
                                                   String orderNo) {
        orderRepository.markStockReleased(orderNo);
        outboxRepository.resolveDead(
                deadOutboxId,
                "Redis stock release confirmed"
        );
    }
}
