package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.service.OrderTxService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 延时 MQ 的兜底。
 *
 * 即使 CLOSE_ORDER outbox 最终 DEAD、RocketMQ 故障或延时消息丢失，
 * DB 中 expire_time 到期的 WAIT_PAY 订单仍会被这里关闭。
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
