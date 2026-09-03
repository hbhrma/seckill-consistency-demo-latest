if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
    return 1
end

return 0
