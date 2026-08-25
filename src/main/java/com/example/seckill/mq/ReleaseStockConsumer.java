package com.example.seckill.mq;

import com.example.seckill.domain.OrderStatus;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.ReleaseStockMessage;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.redis.ReleaseResult;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.util.Jsons;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(
        topic = "${seckill.mq.release-stock-topic}",
        consumerGroup = "${seckill.mq.release-stock-consumer-group}",
        maxReconsumeTimes = 16
)
public class ReleaseStockConsumer implements RocketMQListener<MessageExt> {

    private final OrderRepository orderRepository;
    private final RedisReservationService reservationService;
    private final Jsons jsons;

    public ReleaseStockConsumer(OrderRepository orderRepository,
                                RedisReservationService reservationService,
                                Jsons jsons) {
        this.orderRepository = orderRepository;
        this.reservationService = reservationService;
        this.jsons = jsons;
    }

    @Override
    public void onMessage(MessageExt messageExt) {
        ReleaseStockMessage message =
                jsons.fromJson(messageExt.getBody(), ReleaseStockMessage.class);

        /*
         * 在真正操作 Redis 前再次以 DB 为真相源检查订单必须是 CANCELED。
         * 避免任何错误/伪造/过期 release 消息释放正常订单库存。
         */
        SeckillOrder order = orderRepository.findByOrderNo(message.orderNo())
                .orElseThrow(() -> new IllegalStateException(
                        "release event but order not found: " + message.orderNo()));

        // 这里实际上可以抛出异常
        if (order.status() != OrderStatus.CANCELED) {
            return;
        }

        ReleaseResult result =
                reservationService.releaseCanceled(message.orderNo());

        if (result == ReleaseResult.MISSING
                || result == ReleaseResult.INVALID_STATE
                || result == ReleaseResult.UNKNOWN) {
            throw new IllegalStateException(
                    "cannot confirm Redis release, result=" + result
                            + ", orderNo=" + message.orderNo());
        }

        /*
         * Redis release 是 Lua 幂等的：
         * 即使消息重复消费，这里最多真正 INCR 一次。
         *
         * Redis 成功后再标记 DB stock_released=1。
         * 如果 DB 更新失败，MQ 会重试；下次 Lua 返回 ALREADY_RELEASED，
         * 然后继续把 DB 标记补上。
         */
        if (result == ReleaseResult.RELEASED_NOW
                || result == ReleaseResult.ALREADY_RELEASED) {
            orderRepository.markStockReleased(message.orderNo());
        }
    }
}
