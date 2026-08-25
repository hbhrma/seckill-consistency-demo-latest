package com.example.seckill.mq;

import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.redis.ClaimResult;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.service.CreateOrderTxResult;
import com.example.seckill.service.OrderConsistencyService;
import com.example.seckill.service.OrderTxService;
import com.example.seckill.util.Jsons;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RocketMQMessageListener(
        topic = "${seckill.mq.create-order-topic}",
        consumerGroup = "${seckill.mq.create-order-consumer-group}",
        maxReconsumeTimes = 5
)
public class CreateOrderConsumer implements RocketMQListener<MessageExt> {

    private final OrderRepository orderRepository;
    private final OrderTxService orderTxService;
    private final OrderConsistencyService consistencyService;
    private final RedisReservationService reservationService;
    private final Jsons jsons;

    public CreateOrderConsumer(OrderRepository orderRepository,
                               OrderTxService orderTxService,
                               OrderConsistencyService consistencyService,
                               RedisReservationService reservationService,
                               Jsons jsons) {
        this.orderRepository = orderRepository;
        this.orderTxService = orderTxService;
        this.consistencyService = consistencyService;
        this.reservationService = reservationService;
        this.jsons = jsons;
    }

    @Override
    public void onMessage(MessageExt messageExt) {
        CreateOrderMessage message =
                jsons.fromJson(messageExt.getBody(), CreateOrderMessage.class);

        /*
         * 第一层幂等：DB 已经有订单时，不再创建，直接按订单状态修复 Redis。
         */
        Optional<SeckillOrder> existing =
                orderRepository.findByOrderNo(message.orderNo());

        if (existing.isPresent()) {
            consistencyService.syncFromExistingOrder(existing.get());
            return;
        }

        /*
         * Redis lease 只减少重复消费者同时冲 DB 的概率；真正安全边界是 MySQL guard。
         */
        ClaimResult claim =
                reservationService.claimCreating(message.orderNo());

        if (claim == ClaimResult.ALREADY_ORDERED) {
            /*
             * 系统不变量：reservation=ORDERED 只能发生在 DB 订单已经提交之后。
             * 因此这里重新查 DB 只是为了覆盖“第一次 DB 查询早于另一消费者提交”的竞态。
             * 如果第二次仍查不到订单，不再自动 ABORT / 回补库存，而是直接认为不变量被破坏。
             */
            SeckillOrder orderedDb = orderRepository.findByOrderNo(message.orderNo())
                    .orElseThrow(() -> new IllegalStateException(
                            "invariant violated: Redis reservation is ORDERED but DB order is missing, orderNo="
                                    + message.orderNo()));

            consistencyService.syncFromExistingOrder(orderedDb);
            return;
        }

        if (claim == ClaimResult.RELEASED) {
            return;
        }
        if (claim == ClaimResult.MISSING) {
            throw new IllegalStateException(
                    "reservation missing, refuse to create order: " + message.orderNo());
        }
        if (claim == ClaimResult.BUSY) {
            throw new IllegalStateException(
                    "reservation lease is still valid: " + message.orderNo());
        }
        if (claim != ClaimResult.CLAIMED) {
            throw new IllegalStateException(
                    "unknown claim result=" + claim + ", orderNo=" + message.orderNo());
        }

        final CreateOrderTxResult txResult;

        try {
            txResult = orderTxService.createOrderAndCloseOutbox(message);

        } catch (RuntimeException dbFailure) {
            /*
             * 异常不能证明事务一定没提交（例如 COMMIT 成功但响应丢失）。
             * 所以先重新查订单，以 DB 事实为准。
             */
            try {
                Optional<SeckillOrder> order =
                        orderRepository.findByOrderNo(message.orderNo());

                if (order.isPresent()) {
                    consistencyService.syncFromExistingOrder(order.get());
                    return;
                }
            } catch (RuntimeException queryFailure) {
                dbFailure.addSuppressed(queryFailure);
                throw dbFailure;
            }

            /*
             * DB 明确没有订单时也不把 CREATING 回滚为 RESERVED。
             * 保持 CREATING，等 lease 到期后 RocketMQ 重试重新 claim。
             */
            throw dbFailure;
        }

        if (txResult == CreateOrderTxResult.ABORTED_BY_RECONCILIATION) {
            /*
             * guard 已经 ABORTED。普通 RESERVED/CREATING 会直接释放；
             * 如果 Redis 却是 ORDERED，统一进入冲突分析/修复逻辑。
             */
            consistencyService.releaseAfterAbortedFence(message.orderNo());
            return;
        }

        if (txResult == CreateOrderTxResult.ALREADY_CREATED) {
            SeckillOrder order = orderRepository.findByOrderNo(message.orderNo())
                    .orElseThrow(() -> new IllegalStateException(
                            "guard is CREATED but order missing, orderNo="
                                    + message.orderNo()));
            consistencyService.syncFromExistingOrder(order);
            return;
        }

        if (txResult != CreateOrderTxResult.CREATED_NOW) {
            throw new IllegalStateException(
                    "unexpected create-order tx result=" + txResult
                            + ", orderNo=" + message.orderNo());
        }

        /*
         * CREATED_NOW：DB order + CLOSE_ORDER outbox + guard=CREATED 已提交。
         * CREATING -> ORDERED 是正常路径；如果此时 Redis 已经 RELEASED，
         * 可能是订单并发变为 CANCELED 并完成了库存释放，统一交给一致性服务重新查 DB 裁决。
         */
        consistencyService.markOrderedAfterConfirmedOrder(message.orderNo());
    }
}
