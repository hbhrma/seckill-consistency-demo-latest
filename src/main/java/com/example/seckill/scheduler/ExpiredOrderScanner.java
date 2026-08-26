package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.service.OrderTxService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * DB 级超时订单最终兜底任务。
 *
 * 正常情况下，订单由 RocketMQ CLOSE_ORDER 定时消息在 expireTime
 * 附近触发关闭。
 *
 * 如果 CLOSE_ORDER Outbox 因发送失败次数过多进入 DEAD，
 * 则由 Outbox DEAD Recovery 负责补偿处理。
 *
 * 本任务不依赖 MQ 或 Outbox 状态，属于数据库层面的最终兜底。
 * 周期扫描已经超过 expire_time 但仍处于 WAIT_PAY 状态的订单，并执行：
 *
 * WAIT_PAY -> CANCELED
 * +
 * 创建 RELEASE_STOCK Outbox
 *
 * 用于兜底 Broker 异常、Consumer 长时间不可用、消费多次失败进入 DLQ，
 * 或其他异常导致 CLOSE_ORDER 链路未能最终关闭订单的情况。
 */
@Component
public class ExpiredOrderScanner {

    private final OrderRepository orderRepository;
    private final OrderTxService orderTxService;
    private final SeckillProperties properties;

    public ExpiredOrderScanner(OrderRepository orderRepository,
                               OrderTxService orderTxService,
                               SeckillProperties properties) {
        this.orderRepository = orderRepository;
        this.orderTxService = orderTxService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void scan() {
        for (String orderNo :
                orderRepository.findExpiredWaitingOrderNos(
                        properties.getScheduler().getBatchSize())) {
            try {
                orderTxService.closeExpiredOrderAndCreateReleaseOutbox(orderNo);
            } catch (RuntimeException ignored) {
                // 下一轮继续扫描。生产环境应记录 error/metric/alert。
            }
        }
    }
}
