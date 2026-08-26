package com.example.seckill.service;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.OutboxEventType;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.CloseOrderMessage;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.dto.ReleaseStockMessage;
import com.example.seckill.repository.OrderCreateGuardRepository;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.repository.OutboxRepository;
import com.example.seckill.util.Jsons;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderTxService {

    private final OrderRepository orderRepository;
    private final OrderCreateGuardRepository guardRepository;
    private final OutboxRepository outboxRepository;
    private final Jsons jsons;
    private final SeckillProperties properties;

    public OrderTxService(OrderRepository orderRepository,
                          OrderCreateGuardRepository guardRepository,
                          OutboxRepository outboxRepository,
                          Jsons jsons,
                          SeckillProperties properties) {
        this.orderRepository = orderRepository;
        this.guardRepository = guardRepository;
        this.outboxRepository = outboxRepository;
        this.jsons = jsons;
        this.properties = properties;
    }

    /**
     * 核心本地事务：
     * guard 竞争 + order + CLOSE_ORDER outbox + guard=CREATED 同事务提交。
     */
    @Transactional
    public CreateOrderTxResult createOrderAndCloseOutbox(CreateOrderMessage message) {
        String guardState = guardRepository.lockOrCreate(
                message.orderNo(),
                OrderCreateGuardRepository.CREATING
        );

        if (OrderCreateGuardRepository.ABORTED.equals(guardState)) {
            return CreateOrderTxResult.ABORTED_BY_RECONCILIATION;
        }

        if (OrderCreateGuardRepository.CREATED.equals(guardState)) {
            return CreateOrderTxResult.ALREADY_CREATED;
        }

        if (!OrderCreateGuardRepository.CREATING.equals(guardState)) {
            throw new IllegalStateException(
                    "unknown order-create guard state=" + guardState
                            + ", orderNo=" + message.orderNo());
        }

//        /*
//         * 历史/极端数据兼容：订单已经存在但 guard 缺失/刚补成 CREATING。
//         * 当前系统规定订单与 CLOSE_ORDER outbox 同事务提交，因此这里只修 guard，
//         * 不再额外检查/补写 CLOSE_ORDER outbox。
//         */
//        Optional<SeckillOrder> existing =
//                orderRepository.findByOrderNo(message.orderNo());
//
//        if (existing.isPresent()) {
//            if (guardRepository.markCreated(message.orderNo()) != 1) {
//                throw new IllegalStateException(
//                        "failed to mark existing order guard CREATED, orderNo="
//                                + message.orderNo());
//            }
//            return CreateOrderTxResult.ALREADY_CREATED;
//        }

        // 这是本次新订单唯一一次计算支付截止时间的地方。
        LocalDateTime expireTime = LocalDateTime.now()
                .plusSeconds(properties.getPaymentTimeoutSeconds());

        orderRepository.insertWaitingOrder(
                message.orderNo(),
                message.userId(),
                message.goodsId(),
                expireTime
        );

        long expireAt = expireTime
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();

        CloseOrderMessage closeMessage =
                new CloseOrderMessage(message.orderNo(), expireAt);

        // 正常创建路径使用严格 insert：order 与 CLOSE_ORDER outbox 必须一起成功。
        outboxRepository.insert(
                UUID.randomUUID().toString(),
                "CLOSE_ORDER:" + message.orderNo(),
                OutboxEventType.CLOSE_ORDER,
                jsons.toJson(closeMessage),
                LocalDateTime.now()
        );

        if (guardRepository.markCreated(message.orderNo()) != 1) {
            throw new IllegalStateException(
                    "failed to mark guard CREATED, orderNo=" + message.orderNo());
        }

        return CreateOrderTxResult.CREATED_NOW;
    }

    /**
     * 对账/DLQ/补偿在释放“未成单库存”前必须先拿这个 DB 栅栏。
     */
    @Transactional
    public boolean tryAbortOrderCreation(String orderNo) {
        // 这里边不加事务我觉得也行
        // 最好加上
        if (orderRepository.findByOrderNo(orderNo).isPresent()) {
            return false;
        }

        String guardState = guardRepository.lockOrCreate(
                orderNo,
                OrderCreateGuardRepository.ABORTED
        );

        if (OrderCreateGuardRepository.ABORTED.equals(guardState)) {
            return true;
        }

        if (OrderCreateGuardRepository.CREATED.equals(guardState)) {
            return false;
        }

        throw new IllegalStateException(
                "unexpected persisted guard state=" + guardState
                        + ", orderNo=" + orderNo);
    }

    /**
     * 只有 WAIT_PAY -> CANCELED 成功，才在同一事务写 RELEASE_STOCK outbox。
     */
    // 如果是true，不需要操作
    // 如果return false，那么最好closeExpiredOrderAndCreateReleaseOutbox调用者能够判断下当前库当中订单的状态
    // 如果是paid / canceled，那么什么都不用做
    // 如果是wait pay，抛异常
    @Transactional
    public boolean closeExpiredOrderAndCreateReleaseOutbox(String orderNo) {
        int affected = orderRepository.tryCloseExpired(orderNo);
        if (affected == 0) {
            return false;
        }

        SeckillOrder order = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "order disappeared: " + orderNo));

        ReleaseStockMessage releaseMessage =
                new ReleaseStockMessage(order.orderNo(), order.userId(), order.goodsId());

        outboxRepository.insert(
                UUID.randomUUID().toString(),
                "RELEASE_STOCK:" + orderNo,
                OutboxEventType.RELEASE_STOCK,
                jsons.toJson(releaseMessage),
                LocalDateTime.now()
        );

        return true;
    }

    @Transactional
    public boolean pay(String orderNo) {
        return orderRepository.tryPay(orderNo) == 1;
    }

    public Optional<SeckillOrder> findOrder(String orderNo) {
        return orderRepository.findByOrderNo(orderNo);
    }
}
