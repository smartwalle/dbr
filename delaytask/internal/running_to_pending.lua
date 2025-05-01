-- KEYS[1] - 处理中队列
-- KEYS[2] - 待消费队列
-- KEYS[3] - 消费者队列
-- ARGV[1] - 当前时间(毫秒)

local runningKey = KEYS[1]
local pendingKey = KEYS[2]
local consumerKey = KEYS[3]
local now = ARGV[1]

local toRetry = function(mKey)
    -- 判断消息结构是否存在
    local found = redis.call('EXISTS', mKey)
    if (found == 0) then
        return
    end

    -- 获取消费者
    local consumer = redis.call('HGET', mKey, 'consumer') or ''
    if consumer ~= '' then
        -- 获取消费者的有效时间
        local consumerTimeout = tonumber(redis.call('ZSCORE', consumerKey, consumer) or 0)
        if consumerTimeout > now then
            -- 如果消费者的有效时间大于当前时间，则更新[处理中队列]中消息的消费超时时间
            redis.call('ZADD', runningKey, consumerTimeout, mKey)
            return
        end
    end

    -- 获取剩余重试次数
    local retryRemain = tonumber(redis.call('HGET', mKey, 'retry_remain') or 0)
    if retryRemain > 0 then
        -- 剩余重试次数大于 0
        -- 更新剩余重试次数
        redis.call('HINCRBY', mKey, 'retry_remain', -1)
        -- 清除消费者
        redis.call('HSET', mKey, 'consumer', '')

        local retryTime = now

        -- 获取重试延迟时间
        local retryDelay = tonumber(redis.call('HGET', mKey, 'retry_delay') or 0)
        if retryDelay > 0 then
            retryTime = retryTime + retryDelay * 1000
        end

        -- 获取消息uuid
        local uuid = redis.call('HGET', mKey, 'uuid') or ''
        if  uuid ~= '' then
            -- 添加到[待消费队列]中
            redis.call('ZADD', pendingKey, retryTime, uuid)
        end
    else
        -- 删除[消息结构]
        redis.call('DEL', mKey)
    end
    -- 从[处理中队列]中删除消息
    redis.call('ZREM', runningKey, mKey)
end

-- 获取[处理中队列]中已经消费超时的消息
local mKeys = redis.call('ZRANGEBYSCORE', runningKey, '-inf', now)
if (#mKeys > 0) then
    for _, mKey in ipairs(mKeys) do
        if (mKey ~= nil and mKey ~= '') then
            -- 重试处理逻辑
            toRetry(mKey)
        end
    end
end