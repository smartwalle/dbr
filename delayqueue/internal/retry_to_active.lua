-- KEYS[1] - 待重试队列
-- KEYS[2] - 处理中队列
-- KEYS[3] - 消费者队列
-- ARGV[1] - 消费者id

local toActive= function(mKey)
    -- 判断消息结构是否存在
    local found = redis.call('EXISTS', mKey)
    if (found == 0) then
        return ''
    end

    -- 获取消费者的有效时间
    local consumerTimeout = redis.call('ZSCORE', KEYS[3], ARGV[1])
    if (consumerTimeout ~= nil and consumerTimeout ~= '') then
        -- 获取消息 uuid
        local uuid = redis.call('HGET', mKey, 'uuid')
        -- 设置消费者id
        redis.call('HSET', mKey, 'cid', ARGV[1])
        -- 添加到[处理中队列]
        redis.call('ZADD', KEYS[2], consumerTimeout, mKey)
        -- 从[待重试队列]中删除消息
        redis.call('ZREM', KEYS[1], mKey)
        return uuid
    end
    return ''
end

-- 获取当前时间
local now = redis.call('TIME')
local milliseconds = now[1] * 1000 + math.floor(now[2] / 1000)

-- 获取[待重试队列]中已到重试时间的消息
local mKeys = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', milliseconds, 'LIMIT', 0, 1)
if (#mKeys > 0) then
    for _, mKey in ipairs(mKeys) do
        if (mKey ~= nil and mKey ~= '') then
            return toActive(mKey, milliseconds)
        end
    end
end
return ''