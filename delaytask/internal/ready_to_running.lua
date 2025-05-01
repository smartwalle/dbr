-- KEYS[1] - 就绪队列
-- KEYS[2] - 处理中队列
-- KEYS[3] - 消费者队列
-- ARGV[1] - 消费者

local readyKey = KEYS[1]
local runningKey = KEYS[2]
local consumerKey = KEYS[3]
local consumer = ARGV[1]

local mKey = redis.call('LPOP', readyKey) or ''
if mKey == '' then
    return ''
end
-- 判断消息结构是否存在
local found = redis.call('EXISTS', mKey)
if found == 0 then
    return ''
end

-- 获取消费者的有效时间
local consumerTimeout = tonumber(redis.call('ZSCORE', consumerKey, consumer)) or 0
if consumerTimeout > 0 then
    -- 获取消息 uuid
    local uuid = redis.call('HGET', mKey, 'uuid')
    -- 设置消费者
    redis.call('HSET', mKey, 'consumer', consumer)
    -- 添加到[处理中队列]
    redis.call('ZADD', runningKey, consumerTimeout, mKey)
    return uuid
end
return ''