package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.OutboxEventType;
import com.example.seckill.domain.OutboxMessage;
import com.example.seckill.dto.CloseOrderMessage;
import com.example.seckill.repository.OutboxRepository;
import com.example.seckill.util.Jsons;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class OutboxPublisherScheduler {

    private final OutboxRepository outboxRepository;
    private final RocketMQTemplate rocketMQTemplate;
    private final SeckillProperties properties;
    private final Jsons jsons;

    public OutboxPublisherScheduler(OutboxRepository outboxRepository,
                                    RocketMQTemplate rocketMQTemplate,
                                    SeckillProperties properties,
                                    Jsons jsons) {
        this.outboxRepository = outboxRepository;
        this.rocketMQTemplate = rocketMQTemplate;
        this.properties = properties;
        this.jsons = jsons;
    }
    /*
     * NEW：刚写入 Outbox，还没尝试发送
     * SENDING：已经被某个 Publisher 抢到，正在发送
     * RETRY：上一次发送失败，等待下次重试
     * SENT：已经确认成功发送到 RocketMQ Broker，是发送到broker，而不是说发送到消费者
     * DEAD：重试次数达到上限，正常发送链路停止
     * RESOLVED：MQ 投递失败了，但这条消息已经没有业务价值，问题已闭环，比如，close_order 消息DEAD后，发现订单是paid
     */



    /*
     * 首先会更新outbox中长时间处于spending（被别的任务处理的消息）的消息，更改其状态为retry，表示可以重新被任务处理
     * 防止sending消息因为处理任务宕机而饿死
     *
     * 之后任务会从outbox中取出当前可以处理的消息
     * 然后尝试获取消息的处理权（retry / new -> sending)
     * 之后，对每个消息判断类型：
     * 1. 消息是close_order，表示这是一个截止支付消息，如果该消息已经到了要截止支付的时间了，那么发送该消息；如果该消息还没有到达截止支付的时间，
     * 通过syncSendDeliverTimeMills方法，producer（任务）把消息先投递到broker，然后由broker在指定时间发送给消费者（closeOrderConsumer）
     * 2. 如果消息是RELEASE_STOCK，那么就证明，存在订单，因为到了截止支付时间而被closeOrderConsumer关闭，该消费者
     * 关闭订单后（修改订单状态为CANCELED），需要向outbox中插入一个消息，类型是release_stock，用于向broker发送回补库存的消息
     * 那么相关的消费者消费消息回补库存。
     * 不管是哪种类型的消息，如果发送不成功，都需要重新发送（不过重新发送有上限）
     */

    // 这边消息发送的可靠性是不止基于producer的重试机制，业务层自己也会做重试
    // 不过达到业务层最大重试次数便不再重试，进入DEAD状态由dead处理任务处理
    // 最终兜底是canceledorderreleasereconciler 和 expiredorderscanner
    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void publish() {
        // recoverStuckSending 用于处理某个producdr修改outbox记录状态为sending后挂掉导致消息一致没有办法
        // 发送的情况。此时将消息的状态重新修改为retry，并且记录错误原因
        outboxRepository.recoverStuckSending(
                properties.getScheduler().getSendingTimeoutSeconds());

        for (OutboxMessage outbox :
                outboxRepository.findDue(properties.getScheduler().getBatchSize())) {

            if (outboxRepository.tryClaim(outbox.id()) != 1) {
                continue;
            }
            // outbox中的payload包括对应订单的订单号和截止支付时间戳
            try {
                Message<String> message = MessageBuilder
                        .withPayload(outbox.payload())
                        .setHeader(MessageConst.PROPERTY_KEYS, outbox.eventId())
                        .build();

                SendResult result;
                if (outbox.eventType() == OutboxEventType.CLOSE_ORDER) {
                    result = publishCloseOrder(outbox, message);
                } else if (outbox.eventType() == OutboxEventType.RELEASE_STOCK) {
                    result = rocketMQTemplate.syncSend(
                            properties.getMq().getReleaseStockTopic(),
                            message
                    );
                } else {
                    throw new IllegalStateException(
                            "unsupported outbox event=" + outbox.eventType());
                }

                /*
                 * 非 SEND_OK 也不能当成“明确没发送”，因此选择重试而不是回滚业务。
                 * 即使 Broker 实际已收到，重复消息由 Consumer 幂等处理。
                 */
                if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
                    throw new IllegalStateException(
                            "RocketMQ send status is not SEND_OK, eventId="
                                    + outbox.eventId() + ", result=" + result);
                }

                outboxRepository.markSent(outbox.id());

            } catch (RuntimeException e) {
                // 这里抛出的异常应该剔除因为上边消息类型非法的异常
                // 这里修改的逻辑，主要是把重试次数自增，判断是否超过最大重试次数放到数据库中，保证原子性，避免某个publisher用旧的重试次数覆盖
                // 即使不修改也没问题，因为有兜底机制
                // 这里的退避时间还是按照可能不准确的nextRetry，不过这个问题不大
                int nextRetry = outbox.retryCount() + 1;
                outboxRepository.markRetry(
                        outbox.id(),
                        properties.getScheduler().getOutboxMaxRetry(),
                        LocalDateTime.now().plusSeconds(backoffSeconds(nextRetry)),
                        e.getMessage()
                );
            }
        }
    }

    private SendResult publishCloseOrder(OutboxMessage outbox,
                                         Message<String> message) {
        CloseOrderMessage close =
                jsons.fromJson(outbox.payload(), CloseOrderMessage.class);

        long now = System.currentTimeMillis();

        // 修复出来的历史 WAIT_PAY 若已经过期，立即发关单；否则沿用订单原截止时间。
        // 实际上这里可以不发送消息直接关单。然后向outbox中插入回补库存消息记录
        if (close.expireAtEpochMillis() <= now) {
            return rocketMQTemplate.syncSend(
                    properties.getMq().getCloseOrderTopic(),
                    message
            );
        }
        // 哦哦我懂了是，broker到某个时间发送，producer立刻投递到broker
        return rocketMQTemplate.syncSendDeliverTimeMills(
                properties.getMq().getCloseOrderTopic(),
                message,
                close.expireAtEpochMillis()
        );
    }

    private long backoffSeconds(int retryCount) {
        long base = properties.getScheduler().getRetryBaseSeconds();
        long multiplier = 1L << Math.min(retryCount - 1, 6);
        return Math.min(base * multiplier, 300);
    }
}
