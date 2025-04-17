-- KEYS[1] - 消费者队列

-- 获取当前时间
local now = redis.call('TIME')
local milliseconds = now[1] * 1000 + math.floor(now[2] / 1000)

-- 从[消费者队列]删除超时的消费者
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', milliseconds)