 -- KEYS[1] - 待消费队列
 -- KEYS[2] - 消息结构
 -- ARGV[1] - 消息 id (id)
 -- ARGV[2] - 消息 uuid (uuid)
 -- ARGV[3] - 投递时间(毫秒)（deliver_at）
 -- ARGV[4] - 队列名称 (queue)
 -- ARGV[5] - 消息内容 (body)
 -- ARGV[6] - 剩余重试次数 (retry_remain)
 -- ARGV[7] - 重试延迟时间（秒）(retry_delay)
 -- 消息创建时间 (created_at)

-- 添加到[待消费队列]
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
-- 获取当前时间
local now = redis.call('TIME')
local second = tonumber(now[1])
-- 写入消息结构
redis.call('HMSET', KEYS[2], 'id', ARGV[1], 'uuid', ARGV[2], 'deliver_at', ARGV[3], 'queue', ARGV[4], 'body', ARGV[5], 'retry_remain', ARGV[6], 'retry_delay', ARGV[7], 'created_at', second)