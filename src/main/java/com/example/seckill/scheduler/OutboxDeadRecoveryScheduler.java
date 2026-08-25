package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.OutboxMessage;
import com.example.seckill.repository.OutboxRepository;
import com.example.seckill.service.OutboxDeadRecoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Outbox DEAD 业务级补偿周期任务。
 *
 * 普通 Publisher 重试耗尽后，不再盲目根据 MQ 状态决定业务动作，
 * 而是重新查 DB 订单事实，由 OutboxDeadRecoveryService 决定：
 * RETRY、直接关单、释放库存、RESOLVED，或者抛出一致性异常。
 */
@Component
public class OutboxDeadRecoveryScheduler {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxDeadRecoveryScheduler.class);

    private final OutboxRepository outboxRepository;
    private final OutboxDeadRecoveryService recoveryService;
    private final SeckillProperties properties;

    public OutboxDeadRecoveryScheduler(OutboxRepository outboxRepository,
                                       OutboxDeadRecoveryService recoveryService,
                                       SeckillProperties properties) {
        this.outboxRepository = outboxRepository;
        this.recoveryService = recoveryService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void recover() {
        for (OutboxMessage outbox : outboxRepository.findDueDead(
                properties.getScheduler().getBatchSize())) {
            try {
                recoveryService.handle(outbox);
            } catch (RuntimeException e) {
                /*
                 * DEAD 补偿自身失败：
                 * - 不把 DEAD 改成 SENT/RESOLVED；
                 * - 不直接做危险业务推断；
                 * - 只延迟下一次检查，避免高频死循环。
                 *
                 * 如果 DB 查询本身失败，也自然会走到这里。
                 */
                long delaySeconds = Math.max(
                        30L,
                        properties.getScheduler().getRetryBaseSeconds()
                );

                outboxRepository.rescheduleDeadCheck(
                        outbox.id(),
                        LocalDateTime.now().plusSeconds(delaySeconds),
                        e.getMessage()
                );

                log.error(
                        "Outbox DEAD recovery failed, eventId={}, eventType={}, bizKey={}",
                        outbox.eventId(),
                        outbox.eventType(),
                        outbox.bizKey(),
                        e
                );
            }
        }
    }
}
