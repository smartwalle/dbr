-- KEYS[1] - 消费者队列
-- ARGV[1] - 消费者
-- ARGV[2] - 消费者存活时间

return redis.call('ZADD', KEYS[1], 'XX', 'CH', ARGV[2], ARGV[1])