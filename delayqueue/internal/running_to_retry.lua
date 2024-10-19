-- KEYS[1] - 处理中队列
-- KEYS[2] - 待重试队列
-- KEYS[3] - 消费者队列
-- ARGV[1] - 重试延迟时间（秒）

local toRetry = function(mKey, now)
    -- 判断消息结构是否存在
    local found = redis.call('EXISTS', mKey)
    if (found == 0) then
        return
    end

    -- 获取消费者id
    local consumer = redis.call('HGET', mKey, 'cid')
    if (consumer ~= nil and consumer ~= '') then
        -- 获取消费者的有效时间
        local consumerTimeout = redis.call('ZSCORE', KEYS[3], consumer)
        if (consumerTimeout ~= nil and consumerTimeout ~= '') then
            local timeout = tonumber(consumerTimeout)
            if (timeout ~= nil and timeout > now) then
                -- 如果消费者的有效时间大于当前时间，则更新[处理中队列]中消息的消费超时时间
                redis.call('ZADD', KEYS[1], timeout, mKey)
                return
            end
        end
    end

    -- 获取剩余重试次数
    local count = redis.call('HGET', mKey, 'rc')
    if (count ~= nil and count ~= '' and tonumber(count) > 0) then
        -- 剩余重试次数大于 0
        -- 更新剩余重试次数
        redis.call('HINCRBY', mKey, 'rc', -1)
        -- 清除消费者id
        redis.call('HSET', mKey, 'cid', '')
        -- 添加到[待重试队列]中
        local timeout = now+ARGV[1] * 1000
        redis.call('ZADD', KEYS[2], timeout, mKey)
    else
        -- 删除[消息结构]
        redis.call('DEL', mKey)
        -- TODO 记录失败消息
    end
    -- 从[处理中队列]中删除消息
    redis.call('ZREM', KEYS[1], mKey)
end

-- 获取当前时间
local now = redis.call('TIME')
local milliseconds = now[1] * 1000 + math.floor(now[2] / 1000)

-- 获取[处理中队列]中已经消费超时的消息
local mKeys = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', milliseconds)
if (#mKeys > 0) then
    for _, mKey in ipairs(mKeys) do
        if (mKey ~= nil and mKey ~= '') then
            -- 重试处理逻辑
            toRetry(mKey, milliseconds)
        end
    end
end