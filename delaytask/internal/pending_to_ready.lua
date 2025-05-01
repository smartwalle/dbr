-- KEYS[1] - 待消费队列
-- KEYS[2] - 就绪队列
-- KEYS[3] - MessageKeyPrefix
-- ARGV[1] - 当前时间(毫秒)
-- ARGV[2] - 单次处理数量

local pendingKey = KEYS[1]
local readyKey = KEYS[2]
local messagePrefix = KEYS[3]
local now = ARGV[1]
local limit = ARGV[2]

local ids = redis.call('ZRANGEBYSCORE', pendingKey, '-inf', now, 'LIMIT', 0, limit)
if (#ids > 0) then
    for _, id in ipairs(ids) do
        local mKey = messagePrefix..id
        -- 判断消息结构是否存在
        local found = redis.call('EXISTS', mKey)
        if found == 1 then
            local uuid = redis.call('HGET', mKey, 'uuid') or ''
            if  uuid ~= '' then
                local nKey = messagePrefix..uuid
                redis.call('RPUSH', readyKey, nKey)
                if uuid ~= id then
                    -- 消费超时的消息和 nack 的消息被重新添加到[待消费队列]中时，用的是 uuid
                    redis.call('RENAME', mKey, nKey)
                end
            end
        end
        redis.call('ZREM', pendingKey, id)
    end
end