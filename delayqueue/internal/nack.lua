-- KEYS[1] - 处理中队列
-- KEYS[2] - 待重试队列
-- KEYS[3] - MessageKey(uuid)

local mKey = KEYS[3]
-- 从[处理中队列]获取消息的分值，主要用于判断该消息是否还存在于[处理中队列]中
local score = redis.call('ZSCORE', KEYS[1], mKey)
if (not score) then
    return ''
end

-- 判断消息结构是否存在
local found = redis.call('EXISTS', mKey)
if (found == 0) then
    return ''
end

-- 清除消费者id
redis.call('HSET', mKey, 'cid', '')

-- 获取消息 uuid
local uuid = redis.call('HGET', mKey, 'uuid')

-- 获取剩余重试次数
local retryRemainCount = redis.call('HGET', mKey, 'rr')
if retryRemainCount ~= nil and retryRemainCount ~= '' and tonumber(retryRemainCount) > 0 then
    -- 剩余重试次数大于 0
    -- 更新剩余重试次数
    redis.call('HINCRBY', mKey, 'rr', -1)
    -- 清除消费者id
    redis.call('HSET', mKey, 'cid', '')

    -- 获取当前时间
    local now = redis.call('TIME')
    local milliseconds = now[1] * 1000 + math.floor(now[2] / 1000)

    local retryTime = milliseconds

    -- 获取重试延迟时间
    local retryDelay = redis.call('HGET', mKey, 'rd')
    if retryDelay ~= nil and retryDelay ~= '' and tonumber(retryDelay) > 0 then
        retryTime = retryTime + tonumber(retryDelay) * 1000
    end

    -- 添加到[待重试队列]中
    redis.call('ZADD', KEYS[2], retryTime, mKey)
else
    -- 删除[消息结构]
    redis.call('DEL', mKey)
end
-- 从[处理中队列]中删除消息
redis.call('ZREM', KEYS[1], mKey)
return uuid