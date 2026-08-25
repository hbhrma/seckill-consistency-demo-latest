package com.example.seckill.mq;

import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.service.FinalCreateFailureService;
import com.example.seckill.util.Jsons;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * RocketMQ 默认 DLQ topic 形式：%DLQ%{consumerGroup}
 *
 * 创建订单连续失败后，在这里最终确认：
 * DB 没订单 -> 释放 Redis 预占；
 * DB 有订单 -> 修正 Redis 状态，绝不释放。
 */
@Component
@RocketMQMessageListener(
        topic = "%DLQ%${seckill.mq.create-order-consumer-group}",
        consumerGroup = "seckill-create-order-dlq-worker",
        maxReconsumeTimes = 16
)
public class CreateOrderDlqConsumer implements RocketMQListener<MessageExt> {

    private final FinalCreateFailureService failureService;
    private final Jsons jsons;

    public CreateOrderDlqConsumer(FinalCreateFailureService failureService, Jsons jsons) {
        this.failureService = failureService;
        this.jsons = jsons;
    }

    @Override
    public void onMessage(MessageExt messageExt) {
        CreateOrderMessage message =
                jsons.fromJson(messageExt.getBody(), CreateOrderMessage.class);

        failureService.handle(message);
    }
}
