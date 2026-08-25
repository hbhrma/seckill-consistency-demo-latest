-- 重要：这个脚本只能在 MySQL 的 seckill_order_create_guard
-- 已经被原子地确定为 ABORTED 之后调用。
--
-- KEYS[1] stockKey
-- KEYS[2] buyersKey
-- KEYS[3] reservationKey
-- KEYS[4] reconcilePendingZset
-- KEYS[5] createOrderSendPendingZset
-- ARGV[1] orderNo

if redis.call('EXISTS', KEYS[3]) == 0 then
    return -1
end

local state = redis.call('HGET', KEYS[3], 'state')

if state == 'RELEASED' then
    redis.call('ZREM', KEYS[4], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
    return 0
end

-- ORDERED 代表 Redis 认为已有真实订单，普通 unordered release 必须拒绝；
-- Java 层需要重新查询 DB 决定是脏 ORDERED 还是确实存在订单。
if state == 'ORDERED' then
    return 2
end

-- guard 已经 ABORTED 后，RESERVED/CREATING 都可以安全作为“未成单库存”释放。
if state ~= 'RESERVED' and state ~= 'CREATING' then
    return 5
end

local userId = redis.call('HGET', KEYS[3], 'userId')
if not userId then
    return 5
end

redis.call('INCR', KEYS[1])
redis.call('SREM', KEYS[2], userId)
redis.call('HSET', KEYS[3],
    'state', 'RELEASED',
    'leaseUntil', '0'
)
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])

return 1
