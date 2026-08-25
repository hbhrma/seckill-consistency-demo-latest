-- KEYS[1] stockKey
-- KEYS[2] buyersKey
-- KEYS[3] reservationKey
-- KEYS[4] reconcilePendingZset
-- KEYS[5] createOrderSendPendingZset
-- ARGV[1] userId
-- ARGV[2] goodsId
-- ARGV[3] orderNo
-- ARGV[4] nowMillis
-- ARGV[5] firstCheckAtMillis
-- ARGV[6] retentionSeconds
-- ARGV[7] firstSendRetryAtMillis

local stock = tonumber(redis.call('GET', KEYS[1]) or '-1')
if stock <= 0 then
    return -1
end

if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return -2
end

if redis.call('EXISTS', KEYS[3]) == 1 then
    return -3
end

-- 秒杀资格预占必须原子完成：扣库存 + 防重购买 + reservation + 两个待处理索引。
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])

redis.call('HSET', KEYS[3],
    'orderNo', ARGV[3],
    'userId', ARGV[1],
    'goodsId', ARGV[2],
    'state', 'RESERVED',
    'createdAt', ARGV[4],
    'leaseUntil', '0'
)

redis.call('EXPIRE', KEYS[3], tonumber(ARGV[6]))

-- 对账宽限期：到 firstCheckAt 之后才进入对账候选。
redis.call('ZADD', KEYS[4], ARGV[5], ARGV[3])

-- Producer 可靠发送：创建 reservation 的同时就留下“CREATE_ORDER 尚待确认发送”的事实。
redis.call('ZADD', KEYS[5], ARGV[7], ARGV[3])

return 1
