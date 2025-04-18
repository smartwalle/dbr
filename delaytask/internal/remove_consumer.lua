-- KEYS[1] - 消费者队列
-- ARGV[1] - 消费者

return redis.call('ZREM', KEYS[1], ARGV[1])