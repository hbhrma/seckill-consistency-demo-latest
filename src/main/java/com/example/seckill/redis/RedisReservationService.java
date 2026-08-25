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

@Service
public class RedisReservationService {

    private final StringRedisTemplate redisTemplate;
    private final SeckillProperties properties;

    private final DefaultRedisScript<Long> reserveScript;
    private final DefaultRedisScript<Long> claimScript;
    private final DefaultRedisScript<Long> markOrderedScript;
    private final DefaultRedisScript<Long> releaseUnorderedScript;
    private final DefaultRedisScript<Long> releaseCanceledScript;

    public RedisReservationService(StringRedisTemplate redisTemplate,
                                   SeckillProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.reserveScript = script("lua/reserve.lua");
        this.claimScript = script("lua/claim_creating.lua");
        this.markOrderedScript = script("lua/mark_ordered.lua");
        this.releaseUnorderedScript = script("lua/release_unordered.lua");
        this.releaseCanceledScript = script("lua/release_canceled.lua");
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
                        RedisKeys.PENDING_RESERVATION_ZSET,
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
                        RedisKeys.PENDING_RESERVATION_ZSET,
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
                        RedisKeys.PENDING_RESERVATION_ZSET,
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
                RedisKeys.PENDING_RESERVATION_ZSET,
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
        return findDue(RedisKeys.PENDING_RESERVATION_ZSET, limit);
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
                .remove(RedisKeys.PENDING_RESERVATION_ZSET, orderNo);
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
}
