-- KEYS[1] reservationKey
-- KEYS[2] reconcilePendingZset
-- KEYS[3] createOrderSendPendingZset
-- ARGV[1] orderNo
-- ARGV[2] nowMillis
-- ARGV[3] leaseUntilMillis

if redis.call('EXISTS', KEYS[1]) == 0 then
    return -1
end

local state = redis.call('HGET', KEYS[1], 'state')

if state == 'ORDERED' then
    redis.call('ZREM', KEYS[3], ARGV[1])
    return 2
end

if state == 'RELEASED' then
    redis.call('ZREM', KEYS[3], ARGV[1])
    return 3
end

if state == 'RESERVED' then
    redis.call('HSET', KEYS[1],
        'state', 'CREATING',
        'leaseUntil', ARGV[3]
    )
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
    -- 消费者已经拿到消息，说明 Broker 中确实存在 CREATE_ORDER；Producer 不再需要重发。
    redis.call('ZREM', KEYS[3], ARGV[1])
    return 1
end

if state == 'CREATING' then
    local leaseUntil = tonumber(redis.call('HGET', KEYS[1], 'leaseUntil') or '0')

    -- lease 只用于减少重复处理；真正安全边界仍是 MySQL guard。
    if leaseUntil <= tonumber(ARGV[2]) then
        redis.call('HSET', KEYS[1],
            'leaseUntil', ARGV[3]
        )
        redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
        redis.call('ZREM', KEYS[3], ARGV[1])
        return 1
    end

    return 4
end

return -2
