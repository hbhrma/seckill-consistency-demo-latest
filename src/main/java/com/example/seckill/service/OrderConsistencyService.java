package com.example.seckill.service;

import com.example.seckill.domain.OrderStatus;
import com.example.seckill.domain.SeckillOrder;
import com.example.seckill.redis.MarkOrderedResult;
import com.example.seckill.redis.RedisReservationService;
import com.example.seckill.redis.ReleaseResult;
import com.example.seckill.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * DB -> Redis 的统一最终一致性修复入口。
 *
 * 核心原则：
 * 1. DB 订单是订单业务事实源；
 * 2. Redis reservation 允许暂时落后于 DB；
 * 3. reservation=ORDERED 是强不变量：正常协议下 DB 必须已经存在订单；
 * 4. Redis=RELEASED 是合法终态，可能是订单在并发窗口中刚刚 CANCELED 并完成了库存回补，
 *    因此 markOrdered 遇到 RELEASED 时不能反向改回 ORDERED，而要重新读取 DB 最新状态。
 */
@Service
public class OrderConsistencyService {

    private static final Logger log = LoggerFactory.getLogger(OrderConsistencyService.class);

    private final OrderRepository orderRepository;
    private final RedisReservationService reservationService;

    public OrderConsistencyService(OrderRepository orderRepository,
                                   RedisReservationService reservationService) {
        this.orderRepository = orderRepository;
        this.reservationService = reservationService;
    }

    /**
     * DB 已经明确存在订单时，以 DB 业务状态为事实源修正 Redis。
     *
     * 当前系统保证 order + CLOSE_ORDER outbox + guard=CREATED 同事务提交，
     * 因此这里不重复检查/补写 CLOSE_ORDER outbox。
     */
    public void syncFromExistingOrder(SeckillOrder order) {
        switch (order.status()) {
            case WAIT_PAY, PAID -> markOrderedAfterConfirmedOrder(order.orderNo());

            case CANCELED -> {
                ReleaseResult result = reservationService.releaseCanceled(order.orderNo());
                if (result == ReleaseResult.RELEASED_NOW
                        || result == ReleaseResult.ALREADY_RELEASED) {
                    orderRepository.markStockReleased(order.orderNo());
                    return;
                }

                throw new IllegalStateException(
                        "cannot sync canceled order to Redis, result=" + result
                                + ", orderNo=" + order.orderNo());
            }
        }
    }

    /**
     * 已经确认 DB 中存在该订单时，将 reservation 同步为 ORDERED。
     *
     * 正常：CREATING -> ORDERED；ORDERED 幂等。
     * RELEASED：不能反向修改，必须重新查询 DB 最新状态：
     * - 最新 DB=CANCELED：说明并发关单已经完成，RELEASED 正确，只补 stock_released 确认位；
     * - 最新 DB=WAIT_PAY/PAID：说明活动订单的库存却已被释放，是严重一致性异常。
     * RESERVED：当前协议下非法，因为创建 DB 订单之前必须先 claim 为 CREATING。
     */
    public void markOrderedAfterConfirmedOrder(String orderNo) {
        MarkOrderedResult result = reservationService.markOrdered(orderNo);

        if (result == MarkOrderedResult.MARKED_ORDERED
                || result == MarkOrderedResult.ALREADY_ORDERED) {
            return;
        }

        if (result == MarkOrderedResult.ALREADY_RELEASED) {
            resolveReleasedWhileMarkingOrdered(orderNo);
            return;
        }

        if (result == MarkOrderedResult.RESERVED_INVALID) {
            throw new IllegalStateException(
                    "invariant violated: DB order exists but Redis reservation is RESERVED, orderNo="
                            + orderNo);
        }

        throw new IllegalStateException(
                "cannot mark reservation ORDERED, result=" + result
                        + ", orderNo=" + orderNo);
    }

    private void resolveReleasedWhileMarkingOrdered(String orderNo) {
        SeckillOrder latest = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException(
                        "invariant violated: Redis reservation is RELEASED but DB order is missing, orderNo="
                                + orderNo));

        if (latest.status() == OrderStatus.CANCELED) {
            /*
             * 合法并发：调用方之前读到 WAIT_PAY/PAID，但在 markOrdered 前订单已经被关单，
             * releaseCanceled 已把 Redis 变为 RELEASED。此时不应反向改回 ORDERED。
             * Redis 已经确认释放完成，只需把 DB 的确认标志补成 stock_released=1。
             */
            orderRepository.markStockReleased(orderNo);
            return;
        }

        if (latest.status() == OrderStatus.WAIT_PAY
                || latest.status() == OrderStatus.PAID) {
            throw new IllegalStateException(
                    "active DB order but Redis reservation is already RELEASED, orderNo="
                            + orderNo + ", status=" + latest.status());
        }

        throw new IllegalStateException(
                "unexpected DB order status while resolving RELEASED reservation, orderNo="
                        + orderNo + ", status=" + latest.status());
    }

    /**
     * 前置条件：调用方已经通过 MySQL guard 确认该 orderNo = ABORTED。
     *
     * 普通 RESERVED/CREATING 可以按“未成单库存”释放；
     * 如果 Redis 却是 ORDERED，则触发强不变量检查：
     * - DB 无订单：违反 ORDERED => DB order exists，不自动回补，直接抛异常；
     * - DB 有 CANCELED：按已成单后取消路径释放，本质上还是错误；
     * - DB 有 WAIT_PAY/PAID：绝不能释放，抛出严重一致性异常。
     */


    /*
     * 如果 guard 已经为 ABORTED：
     *
     * 1. DB 中不存在订单：
     *    guard 已经保证该 orderNo 不可能再合法建单。
     *    即使 Redis reservation 当前错误地处于 ORDERED，
     *    也可以将其视为脏状态并释放这次预扣库存。
     *
     * 2. DB 中存在订单：
     *    这是严重的不变量冲突：
     *
     *        guard = ABORTED
     *        DB order = present
     *
     *    此时无法判断是 guard 非法还是 DB order 非法，
     *    因而不能自动释放库存，否则可能对真实的
     *    WAIT_PAY / PAID 订单执行 stock++，造成超卖。
     *
     *    应记录异常并进入人工/专门恢复流程。
     */
    public ReleaseResult releaseAfterAbortedFence(String orderNo) {
        Optional<SeckillOrder> dbOrder =
                orderRepository.findByOrderNo(orderNo);

        if (dbOrder.isPresent()) {
            SeckillOrder order = dbOrder.get();

            log.error(
                    "invariant violated: guard=ABORTED but DB order exists, "
                            + "orderNo={}, status={}, stockReleased={}",
                    orderNo,
                    order.status(),
                    order.stockReleased()
            );

            throw new IllegalStateException(
                    "invariant violated: guard=ABORTED but DB order exists, "
                            + "orderNo=" + orderNo
                            + ", status=" + order.status()
            );
        }

        /*
         * 到这里已经确认：
         *
         * guard = ABORTED
         * DB order = missing
         *
         * 因而可以确定：
         * 这次 Redis 预扣库存已经不可能再对应一个合法 DB 订单，
         * 可以安全释放。
         */
        ReleaseResult result =
                reservationService.releaseUnorderedAfterFence(orderNo);

        if (result == ReleaseResult.RELEASED_NOW
                || result == ReleaseResult.ALREADY_RELEASED) {

            /*
             * 注意：
             * 这里不要 markStockReleased()。
             *
             * 这是“未成单库存释放”，DB 中不存在订单。
             * stock_released 只用于真实 CANCELED 订单的库存释放确认。
             */
            return result;
        }

        if (result != ReleaseResult.ORDERED_NOT_ALLOWED) {
            throw new IllegalStateException(
                    "cannot release aborted reservation, result="
                            + result
                            + ", orderNo=" + orderNo
            );
        }

        /*
         * Redis reservation=ORDERED，
         * 但我们前面已经确认：
         *
         * guard = ABORTED
         * DB order = missing
         *
         * 因此这个 ORDERED 不可能代表一个合法已成单状态，
         * 只能视为 Redis 脏状态。
         *
         * 此时允许通过专门的修复 Lua：
         *
         * ORDERED -> RELEASED
         * stock + 1
         * SREM buyer
         * 清理相关 ZSET
         *
         * 不能调用普通 releaseCanceled()，
         * 因为这里根本没有真实的 CANCELED DB order。
         */
        log.error(
                "dirty Redis reservation detected: "
                        + "guard=ABORTED, DB order missing, reservation=ORDERED, "
                        + "orderNo={}",
                orderNo
        );

        ReleaseResult repaired =
                reservationService.releaseOrderedAfterAbortedFence(orderNo);

        if (repaired == ReleaseResult.RELEASED_NOW
                || repaired == ReleaseResult.ALREADY_RELEASED) {
            return repaired;
        }

        throw new IllegalStateException(
                "cannot repair dirty ORDERED reservation after ABORTED fence, "
                        + "result=" + repaired
                        + ", orderNo=" + orderNo
        );
    }
}
