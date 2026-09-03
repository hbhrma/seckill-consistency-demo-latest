package com.example.seckill.service;

import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

// 它的竞争者是创建订单消费者 对账任务
// guard没有办法阻止它和对账任务竞争
// 但是两者的逻辑是一样的，并且两者的逻辑具有幂等性
@Service
public class FinalCreateFailureService {

    private final OrderRepository orderRepository;
    private final OrderTxService orderTxService;
    private final OrderConsistencyService consistencyService;

    public FinalCreateFailureService(OrderRepository orderRepository,
                                     OrderTxService orderTxService,
                                     OrderConsistencyService consistencyService) {
        this.orderRepository = orderRepository;
        this.orderTxService = orderTxService;
        this.consistencyService = consistencyService;}

    /** CREATE_ORDER 重试耗尽进入 DLQ 后的最终处理。 */
    public void handle(CreateOrderMessage message) {
        Optional<SeckillOrder> optional =
                orderRepository.findByOrderNo(message.orderNo());

        if (optional.isPresent()) {
            consistencyService.syncFromExistingOrder(optional.get());
            return;
        }

        boolean aborted =
                orderTxService.tryAbortOrderCreation(message.orderNo());

        if (!aborted) {
            Optional<SeckillOrder> afterFence =
                    orderRepository.findByOrderNo(message.orderNo());

            if (afterFence.isPresent()) {
                consistencyService.syncFromExistingOrder(afterFence.get());
                return;
            }

            throw new IllegalStateException(
                    "creation guard is not ABORTED but order is still missing, orderNo="
                            + message.orderNo());
        }

        try {
            consistencyService.releaseAfterAbortedFence(message.orderNo());
        } catch (RuntimeException failure) {
            // 可以让对账任务继续进行，因为zset中的记录并没有删除
            // 可以不需要刷新重试时间戳，让对账任务尽快进行
            // 但是releaseAfterAbortedFence内部有几个比较严重的一致性异常，应该考虑解决。
            return;
        }
    }
}
