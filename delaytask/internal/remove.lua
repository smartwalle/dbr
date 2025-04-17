-- KEYS[1] - 待消费队列
-- KEYS[2] - MessageKey(id)
-- ARGV[1] - id

-- 从[待消费队列]删除
redis.call('ZREM', KEYS[1], ARGV[1])
-- 删除消息结构
redis.call('DEL', KEYS[2])