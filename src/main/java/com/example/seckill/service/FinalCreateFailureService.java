package com.example.seckill.service;

import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.dto.CreateOrderMessage;
import com.example.seckill.repository.CompensationRepository;
import com.example.seckill.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class FinalCreateFailureService {

    private final OrderRepository orderRepository;
    private final OrderTxService orderTxService;
    private final OrderConsistencyService consistencyService;
    private final CompensationRepository compensationRepository;

    public FinalCreateFailureService(OrderRepository orderRepository,
                                     OrderTxService orderTxService,
                                     OrderConsistencyService consistencyService,
                                     CompensationRepository compensationRepository) {
        this.orderRepository = orderRepository;
        this.orderTxService = orderTxService;
        this.consistencyService = consistencyService;
        this.compensationRepository = compensationRepository;
    }

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
            /*
             * 如果 DB 仍明确无订单，说明 ABORTED 后的 Redis 释放/修复暂时没完成，
             * 写补偿表继续重试。若 DB 又能查到订单，则让异常暴露，不误记成未成单补偿。
             */
            Optional<SeckillOrder> now =
                    orderRepository.findByOrderNo(message.orderNo());
            if (now.isPresent()) {
                throw failure;
            }

            compensationRepository.insertIfAbsent(
                    message.orderNo(),
                    message.userId(),
                    message.goodsId(),
                    failure.getMessage()
            );
        }
    }
}
