package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.redis.ReleaseResult;
import com.example.seckill.repository.OrderRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * RELEASE_STOCK MQ 的最终兜底。
 *
 * 如果 release outbox 发送多次仍失败甚至 DEAD，
 * DB 里 CANCELED + stock_released=0 会一直留着，
 * 这里直接调用同一个幂等 Lua 释放。
 */
@Component
public class CanceledOrderReleaseReconciler {

    private final OrderRepository orderRepository;
    private final RedisReservationService reservationService;
    private final SeckillProperties properties;

    public CanceledOrderReleaseReconciler(OrderRepository orderRepository,
                                          RedisReservationService reservationService,
                                          SeckillProperties properties) {
        this.orderRepository = orderRepository;
        this.reservationService = reservationService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void reconcile() {
        for (String orderNo :
                orderRepository.findCanceledNotReleasedOrderNos(
                        properties.getScheduler().getBatchSize())) {
            try {
                ReleaseResult result = reservationService.releaseCanceled(orderNo);
                if (result == ReleaseResult.RELEASED_NOW
                        || result == ReleaseResult.ALREADY_RELEASED) {
                    orderRepository.markStockReleased(orderNo);
                }
            } catch (RuntimeException ignored) {
                // Redis 恢复后下一轮继续。生产环境应告警。
            }
        }
    }
}
