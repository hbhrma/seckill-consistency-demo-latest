-- 只能在 DB 已经明确存在订单且订单状态为 CANCELED 后调用。
-- 正常状态应为 ORDERED；CREATING 允许作为“DB 已成单/已取消，但 Redis markOrdered 落后”的异常自愈。
-- RESERVED 在当前协议下非法：DB 创建订单之前必须先 RESERVED -> CREATING，且不再回滚到 RESERVED。
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

if state ~= 'ORDERED' and state ~= 'CREATING' then
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
