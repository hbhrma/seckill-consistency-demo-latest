package com.example.seckill.redis;

import com.example.seckill.config.SeckillProperties;
import com.example.seckill.domain.Reservation;
import com.example.seckill.domain.ReservationState;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.example.seckill.redis.RedisKeys.PENDING_RESERVATION_ZSET;

@Service
public class RedisReservationService {

    private final StringRedisTemplate redisTemplate;
    private final SeckillProperties properties;

    private final DefaultRedisScript<Long> reserveScript;
    private final DefaultRedisScript<Long> claimScript;
    private final DefaultRedisScript<Long> markOrderedScript;
    private final DefaultRedisScript<Long> releaseUnorderedScript;
    private final DefaultRedisScript<Long> releaseCanceledScript;
    private final DefaultRedisScript<Long> releaseOrderedAfterAbortedScript;
    private final DefaultRedisScript<Long> updateZsetIfPresentScript;


    public RedisReservationService(StringRedisTemplate redisTemplate,
                                   SeckillProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.reserveScript = script("lua/reserve.lua");
        this.claimScript = script("lua/claim_creating.lua");
        this.markOrderedScript = script("lua/mark_ordered.lua");
        this.releaseUnorderedScript = script("lua/release_unordered.lua");
        this.releaseCanceledScript = script("lua/release_canceled.lua");
        this.releaseOrderedAfterAbortedScript = script("lua/release_ordered_after_aborted.lua");
        this.updateZsetIfPresentScript = script("lua/update_zset_if_present.lua");
    }

    private static DefaultRedisScript<Long> script(String path) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(path));
        script.setResultType(Long.class);
        return script;
    }

    public ReservationResult reserve(long userId, long goodsId, String orderNo) {
        long now = System.currentTimeMillis();
        // 第一次检查是在创建订单10分钟后
        long firstCheckAt = now
                + Duration.ofSeconds(properties.getReconcileGraceSeconds()).toMillis();
        // 5s
        long firstSendRetryAt = now
                + Duration.ofSeconds(properties.getScheduler().getRetryBaseSeconds()).toMillis();

        Long code = redisTemplate.execute(
                reserveScript,
                List.of(
                        RedisKeys.stock(goodsId),
                        RedisKeys.buyers(goodsId),
                        RedisKeys.reservation(orderNo),
                        PENDING_RESERVATION_ZSET,
                        RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET
                ),
                String.valueOf(userId),
                String.valueOf(goodsId),
                orderNo,
                String.valueOf(now),
                String.valueOf(firstCheckAt),
                String.valueOf(properties.getReservationRetentionSeconds()),
                String.valueOf(firstSendRetryAt)
        );

        if (code == null) return ReservationResult.UNKNOWN;
        return switch (code.intValue()) {
            case 1 -> ReservationResult.SUCCESS;
            case -1 -> ReservationResult.SOLD_OUT;
            case -2 -> ReservationResult.DUPLICATE_USER;
            case -3 -> ReservationResult.DUPLICATE_ORDER;
            default -> ReservationResult.UNKNOWN;
        };
    }

    public ClaimResult claimCreating(String orderNo) {
        long now = System.currentTimeMillis();
        long leaseUntil = now
                + Duration.ofSeconds(properties.getCreatingLeaseSeconds()).toMillis();

        Long code = redisTemplate.execute(
                claimScript,
                List.of(
                        RedisKeys.reservation(orderNo),
                        PENDING_RESERVATION_ZSET,
                        RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET
                ),
                orderNo,
                String.valueOf(now),
                String.valueOf(leaseUntil)
        );

        if (code == null) return ClaimResult.UNKNOWN;
        return switch (code.intValue()) {
            case 1 -> ClaimResult.CLAIMED;
            case 2 -> ClaimResult.ALREADY_ORDERED;
            case 3 -> ClaimResult.RELEASED;
            case 4 -> ClaimResult.BUSY;
            case -1 -> ClaimResult.MISSING;
            default -> ClaimResult.UNKNOWN;
        };
    }

    /**
     * DB 已经明确存在订单后调用。
     *
     * Lua 不会把 RELEASED 反向改成 ORDERED。RELEASED 可能来自订单并发取消，
     * 因此把结果返回给 Java，由 Java 重新查询 DB 最新订单状态后再决定。
     */
    public MarkOrderedResult markOrdered(String orderNo) {
        Long code = redisTemplate.execute(
                markOrderedScript,
                List.of(
                        RedisKeys.reservation(orderNo),
                        PENDING_RESERVATION_ZSET,
                        RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET
                ),
                orderNo
        );

        if (code == null) return MarkOrderedResult.UNKNOWN;
        return switch (code.intValue()) {
            case 1 -> MarkOrderedResult.MARKED_ORDERED;
            case 0 -> MarkOrderedResult.ALREADY_ORDERED;
            case 2 -> MarkOrderedResult.ALREADY_RELEASED;
            case 3 -> MarkOrderedResult.RESERVED_INVALID;
            case -1 -> MarkOrderedResult.MISSING;
            case 4 -> MarkOrderedResult.INVALID_STATE;
            default -> MarkOrderedResult.UNKNOWN;
        };
    }

    /**
     * 只能在 MySQL order-create guard 已经确认 ABORTED 后调用。
     * RESERVED/CREATING 属于正常未成单释放；ORDERED 会拒绝并交给更高层分析。
     */
    public ReleaseResult releaseUnorderedAfterFence(String orderNo) {
        Reservation reservation = getReservation(orderNo);
        if (reservation == null) {
            return ReleaseResult.MISSING;
        }

        Long code = redisTemplate.execute(
                releaseUnorderedScript,
                releaseKeys(reservation, orderNo),
                orderNo
        );
        return mapReleaseCode(code);
    }

    /**
     * 只有 DB 已经明确存在该订单且状态为 CANCELED 时调用。
     */
    public ReleaseResult releaseCanceled(String orderNo) {
        Reservation reservation = getReservation(orderNo);
        if (reservation == null) {
            return ReleaseResult.MISSING;
        }

        Long code = redisTemplate.execute(
                releaseCanceledScript,
                releaseKeys(reservation, orderNo),
                orderNo
        );
        return mapReleaseCode(code);
    }

    private List<String> releaseKeys(Reservation reservation, String orderNo) {
        return List.of(
                RedisKeys.stock(reservation.goodsId()),
                RedisKeys.buyers(reservation.goodsId()),
                RedisKeys.reservation(orderNo),
                PENDING_RESERVATION_ZSET,
                RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET
        );
    }

    private ReleaseResult mapReleaseCode(Long code) {
        if (code == null) return ReleaseResult.UNKNOWN;
        return switch (code.intValue()) {
            case 1 -> ReleaseResult.RELEASED_NOW;
            case 0 -> ReleaseResult.ALREADY_RELEASED;
            case 2 -> ReleaseResult.ORDERED_NOT_ALLOWED;
            case 5 -> ReleaseResult.INVALID_STATE;
            case -1 -> ReleaseResult.MISSING;
            default -> ReleaseResult.UNKNOWN;
        };
    }

    public Reservation getReservation(String orderNo) {
        Map<Object, Object> map = redisTemplate.opsForHash()
                .entries(RedisKeys.reservation(orderNo));

        if (map == null || map.isEmpty()) {
            return null;
        }

        String stateText = String.valueOf(map.get("state"));
        return new Reservation(
                String.valueOf(map.get("orderNo")),
                Long.parseLong(String.valueOf(map.get("userId"))),
                Long.parseLong(String.valueOf(map.get("goodsId"))),
                ReservationState.valueOf(stateText),
                Long.parseLong(String.valueOf(map.getOrDefault("createdAt", "0"))),
                Long.parseLong(String.valueOf(map.getOrDefault("leaseUntil", "0")))
        );
    }

    public List<String> findDuePendingOrderNos(int limit) {
        return findDue(PENDING_RESERVATION_ZSET, limit);
    }

    public List<String> findDueCreateOrderSendOrderNos(int limit) {
        return findDue(RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET, limit);
    }

    private List<String> findDue(String zsetKey, int limit) {
        long now = System.currentTimeMillis();
        Set<String> values = redisTemplate.opsForZSet()
                .rangeByScore(zsetKey, 0, now, 0, limit);

        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(values);
    }

    public void removePendingIndex(String orderNo) {
        redisTemplate.opsForZSet()
                .remove(PENDING_RESERVATION_ZSET, orderNo);
    }

    public void removeCreateOrderSendPending(String orderNo) {
        redisTemplate.opsForZSet()
                .remove(RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET, orderNo);
    }

    public void rescheduleCreateOrderSend(String orderNo, long delayMillis) {
        long nextAttemptAt = System.currentTimeMillis() + Math.max(delayMillis, 0);
        redisTemplate.opsForZSet().add(
                RedisKeys.CREATE_ORDER_SEND_PENDING_ZSET,
                orderNo,
                nextAttemptAt
        );
    }

    public void initializeStock(long goodsId, long stock) {
        redisTemplate.opsForValue().set(
                RedisKeys.stock(goodsId),
                String.valueOf(stock));
        redisTemplate.delete(RedisKeys.buyers(goodsId));
    }

    /**
     * 仅用于严重脏状态修复。调用前必须已经同时确认：
     * 1. MySQL order-create guard = ABORTED；
     * 2. DB 中不存在该 orderNo。
     *
     * 在这个前提下，Redis reservation=ORDERED 不可能再对应合法 DB 订单，
     * 因此允许执行 ORDERED -> RELEASED 并回补库存。
     *
     * 普通业务路径禁止调用该方法；正常 unordered release 仍然拒绝 ORDERED。
     */
    public ReleaseResult releaseOrderedAfterAbortedFence(String orderNo) {
        Reservation reservation = getReservation(orderNo);
        if (reservation == null) {
            return ReleaseResult.MISSING;
        }

        Long code = redisTemplate.execute(
                releaseOrderedAfterAbortedScript,
                releaseKeys(reservation, orderNo),
                orderNo
        );
        return mapReleaseCode(code);
    }

    /**
     * 将 reservation 下一次允许被对账任务扫描的时间推迟到 nextCheckAtMillis。
     *
     * 使用 XX 语义：只有 orderNo 当前仍存在于 PENDING_RESERVATION_ZSET 中时才更新 score。
     *
     * 这样可以避免并发情况下：
     *
     * 1. 其他线程已经完成处理并 ZREM(orderNo)
     * 2. 当前对账线程随后又执行普通 ZADD
     * 3. 把已经删除的 orderNo 重新放回 pending ZSET
     */
    public void reschedulePendingIndex(String orderNo, long nextCheckAtMillis) {

        redisTemplate.execute(
                updateZsetIfPresentScript,
                List.of(PENDING_RESERVATION_ZSET),
                orderNo,
                String.valueOf(nextCheckAtMillis)
        );

        /*
         * false 不一定是异常。
         *
         * 可能在当前线程扫描 reservation 之后，
         * 另一个线程已经完成 ORDERED / RELEASED 等处理，
         * 并把该 orderNo 从 pending ZSET 中删除。
         *
         * 此时不应该重新创建 pending 元素。
         */
    }
}
