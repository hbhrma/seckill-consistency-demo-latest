package com.example.seckill.service;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.util.Jsons;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class CreateOrderMessageSender {

    private final RocketMQTemplate rocketMQTemplate;
    private final SeckillProperties properties;
    private final Jsons jsons;

    public CreateOrderMessageSender(RocketMQTemplate rocketMQTemplate,
                                    SeckillProperties properties,
                                    Jsons jsons) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.properties = properties;
        this.jsons = jsons;
    }

    public SendResult send(CreateOrderMessage payload) {
        Message<String> message = MessageBuilder
                .withPayload(jsons.toJson(payload))
                .setHeader(MessageConst.PROPERTY_KEYS, payload.orderNo())
                .build();

        return rocketMQTemplate.syncSend(
                properties.getMq().getCreateOrderTopic(),
                message
        );
    }
}
