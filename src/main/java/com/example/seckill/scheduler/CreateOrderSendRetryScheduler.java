package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.Reservation;
import com.example.seckill.domain.ReservationState;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.service.CreateOrderMessageSender;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Producer 侧可靠发送 CREATE_ORDER。
 * 解决“Redis 已经预占成功，但首次 syncSend 失败/结果不确定”的窗口。
 */
@Component
public class CreateOrderSendRetryScheduler {

    private final RedisReservationService reservationService;
    private final CreateOrderMessageSender messageSender;
    private final SeckillProperties properties;

    public CreateOrderSendRetryScheduler(RedisReservationService reservationService,
                                         CreateOrderMessageSender messageSender,
                                         SeckillProperties properties) {
        this.reservationService = reservationService;
        this.messageSender = messageSender;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void retry() {
        // 如果之后部署多个retry周期任务，那么有可能a刚读到要retry的消息到本地，
        // b已经对这个消息reschedule（延长它retrynextat时间戳）
        // 这样虽然不会造成问题（就是消息重复发送）
        // 但是如果想解决这个问题，就是要保证findDueCreateOrderSendOrderNos和reschedule是原子的。
        // 也就是把两个操作放在lua中
        for (String orderNo : reservationService.findDueCreateOrderSendOrderNos(
                properties.getScheduler().getBatchSize())) {
            try {
                Reservation reservation = reservationService.getReservation(orderNo);
                // 还是一样的，我觉得reservation == null是一个严重错误
                // 应该持久化，然后从zset中删除，然后交给对应逻辑处理
                if (reservation == null
                        || reservation.state() == ReservationState.ORDERED
                        || reservation.state() == ReservationState.RELEASED
                        || reservation.state() == ReservationState.CREATING) {
                    // CREATING 说明某个消费者已经拿到过消息，Broker 侧投递链路已成立。
                    reservationService.removeCreateOrderSendPending(orderNo);
                    continue;
                }


                reservationService.rescheduleCreateOrderSend(
                        orderNo,
                        properties.getScheduler().getRetryBaseSeconds() * 1000L
                );

                CreateOrderMessage payload = new CreateOrderMessage(
                        reservation.orderNo(),
                        reservation.userId(),
                        reservation.goodsId()
                );

                SendResult result = messageSender.send(payload);
                if (result != null && result.getSendStatus() == SendStatus.SEND_OK) {
                    reservationService.removeCreateOrderSendPending(orderNo);
                } else {
//                    log.warn(
//                            "CREATE_ORDER retry send not confirmed, orderNo={}, sendResult={}",
//                            orderNo,
//                            result
//                    );
                }
            } catch (RuntimeException e) {
//                log.error(
//                        "CREATE_ORDER retry failed, orderNo={}",
//                        orderNo,
//                        e
//                );
            }
        }
    }
}
