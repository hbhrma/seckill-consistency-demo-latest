package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.CompensationTask;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.repository.CompensationRepository;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.service.OrderConsistencyService;
import com.example.seckill.service.OrderTxService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

/** DB 已知需要释放、但 Redis 当时失败后的持久化补偿重试。 */
@Component
public class CompensationScheduler {

    private final CompensationRepository compensationRepository;
    private final OrderRepository orderRepository;
    private final OrderTxService orderTxService;
    private final OrderConsistencyService consistencyService;
    private final SeckillProperties properties;

    public CompensationScheduler(CompensationRepository compensationRepository,
                                 OrderRepository orderRepository,
                                 OrderTxService orderTxService,
                                 OrderConsistencyService consistencyService,
                                 SeckillProperties properties) {
        this.compensationRepository = compensationRepository;
        this.orderRepository = orderRepository;
        this.orderTxService = orderTxService;
        this.consistencyService = consistencyService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void compensate() {
        compensationRepository.recoverStuckProcessing(
                properties.getScheduler().getSendingTimeoutSeconds());

        for (CompensationTask task :
                compensationRepository.findDue(properties.getScheduler().getBatchSize())) {

            if (compensationRepository.tryClaim(task.id()) != 1) {
                continue;
            }

            try {
                Optional<SeckillOrder> dbOrder =
                        orderRepository.findByOrderNo(task.orderNo());

                if (dbOrder.isPresent()) {
                    consistencyService.syncFromExistingOrder(dbOrder.get());
                    compensationRepository.markDone(
                            task.id(),
                            "DB order exists; synchronized by order status");
                    continue;
                }

                boolean aborted = orderTxService.tryAbortOrderCreation(task.orderNo());

                if (!aborted) {
                    Optional<SeckillOrder> afterFence =
                            orderRepository.findByOrderNo(task.orderNo());

                    if (afterFence.isPresent()) {
                        consistencyService.syncFromExistingOrder(afterFence.get());
                        compensationRepository.markDone(
                                task.id(),
                                "order created while compensation was racing");
                        continue;
                    }

                    throw new IllegalStateException(
                            "guard not ABORTED while DB order is missing");
                }

                var result = consistencyService.releaseAfterAbortedFence(task.orderNo());
                compensationRepository.markDone(
                        task.id(),
                        "release result=" + result);

            } catch (RuntimeException e) {
                int nextRetry = task.retryCount() + 1;
                boolean dead =
                        nextRetry >= properties.getScheduler().getCompensationMaxRetry();

                compensationRepository.markRetry(
                        task.id(),
                        nextRetry,
                        dead,
                        LocalDateTime.now().plusSeconds(backoffSeconds(nextRetry)),
                        e.getMessage()
                );
            }
        }
    }

    private long backoffSeconds(int retryCount) {
        long base = properties.getScheduler().getRetryBaseSeconds();
        long multiplier = 1L << Math.min(retryCount - 1, 6);
        return Math.min(base * multiplier, 300);
    }
}
