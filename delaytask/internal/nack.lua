-- KEYS[1] - 处理中队列
-- KEYS[2] - 待消费队列
-- KEYS[3] - MessageKey(uuid)
-- ARGV[1] - 当前时间(毫秒)

local runningKey = KEYS[1]
local pendingKey = KEYS[2]
local messageKey = KEYS[3]
local now = ARGV[1]

-- 从[处理中队列]获取消息的分值，主要用于判断该消息是否还存在于[处理中队列]中
local score = tonumber(redis.call('ZSCORE', runningKey, messageKey) or 0)
if score == 0 then
    return ''
end

-- 判断消息结构是否存在
local found = redis.call('EXISTS', messageKey)
if found == 0 then
    return ''
end

-- 清除消费者
redis.call('HSET', messageKey, 'consumer', '')

-- 获取消息 uuid
local uuid = redis.call('HGET', messageKey, 'uuid')

-- 获取剩余重试次数
local retryRemainCount = tonumber(redis.call('HGET', messageKey, 'retry_remain') or 0)
if retryRemainCount > 0 then
    -- 剩余重试次数大于 0
    -- 更新剩余重试次数
    redis.call('HINCRBY', messageKey, 'retry_remain', -1)
    -- 清除消费者
    redis.call('HSET', messageKey, 'consumer', '')

    local retryTime = now

    -- 获取重试延迟时间
    local retryDelay = tonumber(redis.call('HGET', messageKey, 'retry_delay') or 0)
    if retryDelay > 0 then
        retryTime = retryTime + retryDelay * 1000
    end

    -- 添加到[待消费队列]中
    redis.call('ZADD', pendingKey, retryTime, uuid)
else
    -- 删除[消息结构]
    redis.call('DEL', messageKey)
end
-- 从[处理中队列]中删除消息
redis.call('ZREM', runningKey, messageKey)
return uuid