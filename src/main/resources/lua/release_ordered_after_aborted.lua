-- 严重脏状态专用修复脚本。
--
-- 调用前 Java 层必须已经确认：
-- 1. MySQL seckill_order_create_guard = ABORTED；
-- 2. DB 中不存在该 orderNo。
--
-- 只有在这两个事实同时成立时，Redis 中的 ORDERED 才能被判定为脏状态，
-- 并允许执行 ORDERED -> RELEASED + stock++。
--
-- 普通业务路径禁止调用本脚本。
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

-- 幂等：如果之前已经完成释放，只清理残留索引即可。
if state == 'RELEASED' then
    redis.call('ZREM', KEYS[4], ARGV[1])
    redis.call('ZREM', KEYS[5], ARGV[1])
    return 0
end

-- 这个脚本只负责修复 ORDERED 脏状态。
-- RESERVED / CREATING 应由 release_unordered.lua 处理。
if state ~= 'ORDERED' then
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
