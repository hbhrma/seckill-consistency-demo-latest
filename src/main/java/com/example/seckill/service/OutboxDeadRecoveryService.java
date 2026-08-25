package com.example.seckill.service;

import com.example.seckill.domain.OrderStatus;
import com.example.seckill.domain.OutboxEventType;
import com.example.seckill.domain.OutboxMessage;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.CloseOrderMessage;
import com.example.seckill.dto.ReleaseStockMessage;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.redis.ReleaseResult;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.repository.OutboxRepository;
import com.example.seckill.util.Jsons;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Outbox 普通发送重试耗尽后的业务级兜底。
 *
 * 核心原则：
 * 1. DEAD 只说明 MQ 普通投递链路失败，不能直接推导订单/库存业务结果；
 * 2. 进入 DEAD 后重新查询订单数据库，以 DB 订单状态作为事实源；
 * 3. 只有 DB 明确 CANCELED，才允许 releaseCanceled()；
 * 4. WAIT_PAY -> CANCELED 与 RELEASE_STOCK outbox 必须同 DB 事务。
 */
@Service
public class OutboxDeadRecoveryService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final RedisReservationService reservationService;
    private final DeadRecoveryTxService txService;
    private final Jsons jsons;

    public OutboxDeadRecoveryService(OrderRepository orderRepository,
                                     OutboxRepository outboxRepository,
                                     RedisReservationService reservationService,
                                     DeadRecoveryTxService txService,
                                     Jsons jsons) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.reservationService = reservationService;
        this.txService = txService;
        this.jsons = jsons;
    }

    public void handle(OutboxMessage outbox) {
        if (outbox.eventType() == OutboxEventType.CLOSE_ORDER) {
            handleCloseOrder(outbox);
            return;
        }

        if (outbox.eventType() == OutboxEventType.RELEASE_STOCK) {
            handleReleaseStock(outbox);
            return;
        }

        throw new IllegalStateException(
                "unsupported DEAD outbox event=" + outbox.eventType()
                        + ", eventId=" + outbox.eventId());
    }

    /**
     * CLOSE_ORDER=DEAD：
     * - DB无订单：严重不变量错误；
     * - PAID：无需关单/回补，DEAD -> RESOLVED；
     * - WAIT_PAY且未过期：DEAD -> RETRY，next_retry_at=now；
     * - WAIT_PAY且已过期：CAS CANCELED + RELEASE_STOCK outbox；
     * - CANCELED：直接走幂等 releaseCanceled，再确认 stock_released。
     */
    private void handleCloseOrder(OutboxMessage outbox) {
        CloseOrderMessage closeMessage =
                jsons.fromJson(outbox.payload(), CloseOrderMessage.class);

        String orderNo = closeMessage.orderNo();

        SeckillOrder order = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "CLOSE_ORDER DEAD but DB order missing, orderNo=" + orderNo));

        if (order.status() == OrderStatus.PAID) {
            outboxRepository.resolveDead(
                    outbox.id(),
                    "CLOSE_ORDER no longer needed: order already PAID"
            );
            return;
        }

        /*
        * 实际上如果周期任务处理类型为close order消息（状态为dead），
        * 并且订单的状态是canceled，
        * 那么实际上也可以在outbox中插入一个类型为release_stock的消息记录，
        * 并且修改状态为resolved（两个操作放在一个事务中）
        */
        if (order.status() == OrderStatus.CANCELED) {
            releaseCanceledAndResolve(outbox, order);
            return;
        }

        if (order.status() != OrderStatus.WAIT_PAY) {
            throw new IllegalStateException(
                    "unexpected order status for CLOSE_ORDER DEAD, orderNo="
                            + orderNo + ", status=" + order.status());
        }

        /*
         * 仍在支付窗口内：恢复普通 MQ 投递链路。
         * next_retry_at=NOW，让 OutboxPublisherScheduler 尽快重新发送原来的定时消息。
         * retry_count 不清零，保留历史失败次数。
         */
        if (LocalDateTime.now().isBefore(order.expireTime())) {
            outboxRepository.retryDeadNow(outbox.id());
            return;
        }

        /*
         * 已经过支付截止时间：不再浪费时间重新发送 CLOSE_ORDER。
         * DEAD handler 自己执行最终 DB CAS，并且同事务写 RELEASE_STOCK outbox。
         */
        boolean closed = txService.closeExpiredOrderFromDead(
                outbox.id(),
                orderNo
        );

        if (closed) {
            return;
        }

        /*
         * CAS失败意味着在我们读取 WAIT_PAY 后，订单可能被支付线程/其他关单线程改变。
         * 必须重新查 DB 最新状态，不能继续使用旧的 order 对象。
         */
        SeckillOrder latest = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "order disappeared during CLOSE_ORDER DEAD recovery, orderNo="
                                + orderNo));

        if (latest.status() == OrderStatus.PAID) {
            outboxRepository.resolveDead(
                    outbox.id(),
                    "order became PAID during CLOSE_ORDER DEAD recovery"
            );
            return;
        }

        if (latest.status() == OrderStatus.CANCELED) {
            releaseCanceledAndResolve(outbox, latest);
            return;
        }

        if (latest.status() == OrderStatus.WAIT_PAY) {
            throw new IllegalStateException(
                    "expired WAIT_PAY order could not be canceled, orderNo=" + orderNo);
        }

        throw new IllegalStateException(
                "unexpected latest order status during CLOSE_ORDER DEAD recovery, orderNo="
                        + orderNo + ", status=" + latest.status());
    }

    /**
     * RELEASE_STOCK=DEAD：
     * 必须重新查 DB，且只有 CANCELED 才能继续释放 Redis 库存。
     */
    private void handleReleaseStock(OutboxMessage outbox) {
        ReleaseStockMessage releaseMessage =
                jsons.fromJson(outbox.payload(), ReleaseStockMessage.class);

        String orderNo = releaseMessage.orderNo();

        SeckillOrder order = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "RELEASE_STOCK DEAD but DB order missing, orderNo=" + orderNo));

        if (order.status() != OrderStatus.CANCELED) {
            throw new IllegalStateException(
                    "RELEASE_STOCK DEAD but DB order is not CANCELED, orderNo="
                            + orderNo + ", status=" + order.status());
        }

        releaseCanceledAndResolve(outbox, order);
    }

    /**
     * 订单已经由 DB 明确为 CANCELED 后，才允许调用。
     */
    private void releaseCanceledAndResolve(OutboxMessage deadOutbox,
                                           SeckillOrder order) {
        if (order.stockReleased()) {
            /*
             * DB 已经确认 Redis release 完成，不需要再次访问 Redis。
             */
            outboxRepository.resolveDead(
                    deadOutbox.id(),
                    "stock already released"
            );
            return;
        }

        ReleaseResult result =
                reservationService.releaseCanceled(order.orderNo());

        if (result != ReleaseResult.RELEASED_NOW
                && result != ReleaseResult.ALREADY_RELEASED) {
            throw new IllegalStateException(
                    "cannot release CANCELED order from DEAD recovery, result="
                            + result + ", orderNo=" + order.orderNo());
        }

        txService.confirmStockReleasedAndResolveDead(
                deadOutbox.id(),
                order.orderNo()
        );
    }
}
