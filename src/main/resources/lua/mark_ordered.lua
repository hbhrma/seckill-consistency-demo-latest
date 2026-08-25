-- 只有 DB 已经明确存在该 orderNo 的订单时才能调用。
--
-- 正常状态机：RESERVED -> CREATING -> ORDERED -> RELEASED
-- 这里：
--   CREATING -> ORDERED：正常推进；
--   ORDERED：幂等成功；
--   RELEASED：合法终态，不能反向改成 ORDERED，交给 Java 重新查 DB 最新状态；
--   RESERVED：当前协议下非法，因为 DB 创建订单之前必须先 claim 为 CREATING。
--
-- KEYS[1] reservationKey
-- KEYS[2] reconcilePendingZset
-- KEYS[3] createOrderSendPendingZset
-- ARGV[1] orderNo

if redis.call('EXISTS', KEYS[1]) == 0 then
    return -1
end

local state = redis.call('HGET', KEYS[1], 'state')

if state == 'ORDERED' then
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('ZREM', KEYS[3], ARGV[1])
    return 0
end

if state == 'CREATING' then
    redis.call('HSET', KEYS[1],
        'state', 'ORDERED',
        'leaseUntil', '0'
    )
    redis.call('ZREM', KEYS[2], ARGV[1])
    redis.call('ZREM', KEYS[3], ARGV[1])
    return 1
end

if state == 'RELEASED' then
    return 2
end

if state == 'RESERVED' then
    return 3
end

return 4
