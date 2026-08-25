package com.example.seckill.service;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.dto.SeckillResponse;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.redis.ReservationResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class SeckillEntryService {

    private final RedisReservationService reservationService;
    private final CreateOrderMessageSender messageSender;
    private final SeckillProperties properties;

    public SeckillEntryService(RedisReservationService reservationService,
                               CreateOrderMessageSender messageSender,
                               SeckillProperties properties) {
        this.reservationService = reservationService;
        this.messageSender = messageSender;
        this.properties = properties;
    }

    public SeckillResponse seckill(long userId, long goodsId) {
        String orderNo = UUID.randomUUID().toString().replace("-", "");

        ReservationResult result =
                reservationService.reserve(userId, goodsId, orderNo);

        if (result == ReservationResult.SOLD_OUT) {
            return new SeckillResponse(false, null, "库存不足");
        }
        if (result == ReservationResult.DUPLICATE_USER) {
            return new SeckillResponse(false, null, "该用户已经抢购过该商品");
        }
        if (result != ReservationResult.SUCCESS) {
            return new SeckillResponse(false, null, "Redis 预占失败: " + result);
        }

        CreateOrderMessage payload =
                new CreateOrderMessage(orderNo, userId, goodsId);

        /*
         * reserve.lua 已经原子写入 Producer 待发送 ZSET。
         * SEND_OK 才删除；非 SEND_OK 或异常都保留，让定时任务继续重试。
         * 注意：发送异常/超时不等于 Broker 一定没收到，所以绝不能立即回补库存。
         */
        // 这里我不建议send，因为可能会降低并发度，如果非要send，建议异步发送
        try {
            SendResult sendResult = messageSender.send(payload);

            if (sendResult != null
                    && sendResult.getSendStatus() == SendStatus.SEND_OK) {
                reservationService.removeCreateOrderSendPending(orderNo);
            } else {
                reservationService.rescheduleCreateOrderSend(
                        orderNo,
                        properties.getScheduler().getRetryBaseSeconds() * 1000L
                );
            }
        } catch (RuntimeException sendFailure) {
            // 保留 reservation + send-pending，后台可靠重试；对用户返回“已受理”。
            try {
                reservationService.rescheduleCreateOrderSend(
                        orderNo,
                        properties.getScheduler().getRetryBaseSeconds() * 1000L
                );
            } catch (RuntimeException ignored) {
                // reserve.lua 已经写过 send-pending；Redis 此刻也故障时只能等后续恢复/对账。
            }
        }

        return new SeckillResponse(
                true,
                orderNo,
                "秒杀资格已预占，等待异步创建订单"
        );
    }
}
