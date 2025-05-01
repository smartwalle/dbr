-- KEYS[1] - 消费者队列
-- ARGV[1] - 当前时间(毫秒)

-- 从[消费者队列]删除超时的消费者
return redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])