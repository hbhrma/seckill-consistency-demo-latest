package com.example.seckill.mq;

import com.example.seckill.dto.CloseOrderMessage;
import com.example.seckill.service.OrderTxService;
import com.example.seckill.util.Jsons;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(
        topic = "${seckill.mq.close-order-topic}",
        consumerGroup = "${seckill.mq.close-order-consumer-group}",
        maxReconsumeTimes = 8
)
public class CloseOrderConsumer implements RocketMQListener<MessageExt> {

    private final OrderTxService orderTxService;
    private final Jsons jsons;

    public CloseOrderConsumer(OrderTxService orderTxService, Jsons jsons) {
        this.orderTxService = orderTxService;
        this.jsons = jsons;
    }

    @Override
    public void onMessage(MessageExt messageExt) {
        CloseOrderMessage message =
                jsons.fromJson(messageExt.getBody(), CloseOrderMessage.class);

        /*
         * 如果已经 PAID：条件 UPDATE 影响 0 行，不会释放库存。
         * 如果已经 CANCELED：同样影响 0 行。
         * 只有 WAIT_PAY 且 expire_time 已到，才 CANCELED + 写 RELEASE_STOCK outbox。
         */
        orderTxService.closeExpiredOrderAndCreateReleaseOutbox(message.orderNo());
    }
}
