package com.example.seckill.scheduler;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.Reservation;
import com.example.seckill.domain.ReservationState;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.repository.OrderRepository;
import com.example.seckill.service.OrderConsistencyService;
import com.example.seckill.service.OrderTxService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * reservation 与 DB 的最终对账。
 * firstCheckAt/leaseUntil 只减少无意义竞争；真正关闭竞态窗口的是 MySQL guard。
 */
@Component
public class ReservationReconcileScheduler {

    private final RedisReservationService reservationService;
    private final OrderRepository orderRepository;
    private final OrderTxService orderTxService;
    private final OrderConsistencyService consistencyService;
    private final SeckillProperties properties;

    public ReservationReconcileScheduler(RedisReservationService reservationService,
                                         OrderRepository orderRepository,
                                         OrderTxService orderTxService,
                                         OrderConsistencyService consistencyService,
                                         SeckillProperties properties) {
        this.reservationService = reservationService;
        this.orderRepository = orderRepository;
        this.orderTxService = orderTxService;
        this.consistencyService = consistencyService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${seckill.scheduler.fixed-delay-ms:3000}")
    public void reconcile() {
        for (String orderNo : reservationService.findDuePendingOrderNos(
                properties.getScheduler().getBatchSize())) {

            try {
                Reservation reservation = reservationService.getReservation(orderNo);

                if (reservation == null) {
                    /*
                     * 正常情况下，不应该出现
                     * 但是出现了，那么可以持久化这个错误，之后删除这个zset元素
                     * 防止对账任务每次都要处理这个没法处理的问题
                     * 然后交给专门的处理逻辑
                     */
                    reservationService.removePendingIndex(orderNo);
                    continue;
                }

                // 实际上我觉得，这里可以markStockReleased
                if (reservation.state() == ReservationState.RELEASED) {
                    reservationService.removePendingIndex(orderNo);
                    continue;
                }

                /*
                 * 扫描到 orderNo 之后，消费者可能刚好重新 claim 并延长 lease。
                 * (再)读一次 lease 可以减少与健康消费者竞争 guard，但不能替代 guard。
                 */
                if (reservation.state() == ReservationState.CREATING
                        && reservation.leaseUntilMillis() > System.currentTimeMillis()) {
                    continue;
                }

                Optional<SeckillOrder> dbOrder =
                        orderRepository.findByOrderNo(orderNo);

                if (dbOrder.isPresent()) {
                    consistencyService.syncFromExistingOrder(dbOrder.get());
                    continue;
                }
                // 其实走到这一步，保留状态出现什么都有可能，兜底还是要靠guard
                boolean aborted = orderTxService.tryAbortOrderCreation(orderNo);

                if (!aborted) {
                    Optional<SeckillOrder> orderAfterFence =
                            orderRepository.findByOrderNo(orderNo);

                    if (orderAfterFence.isPresent()) {
                        consistencyService.syncFromExistingOrder(orderAfterFence.get());
                    }
                    // 创建方赢但暂时仍读不到订单时，保守等待下一轮，不释放。
                    /*
                     * 对账任务中，如果!aborted，进行查库，如果发现订单不存在，实际上也是要抛异常的吧？是的，所以这里gpt的注释不对
                     *
                     */
//                    continue; // 抛异常
                    throw new IllegalStateException(
                            "invariant violated: abort fence was not acquired "
                                    + "but DB order is missing, orderNo="
                                    + orderNo
                    );
                }

                consistencyService.releaseAfterAbortedFence(orderNo);

            } catch (RuntimeException ignored) {
                // DB/Redis 异常或严重不变量冲突：不猜测、不强行加库存，下一轮继续/生产告警。
            }
        }
    }
}
