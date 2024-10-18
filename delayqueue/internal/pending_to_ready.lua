-- KEYS[1] - 待消费队列
-- KEYS[2] - 就绪队列
-- KEYS[3] - MessageKeyPrefix
-- ARGV[1] - 单次处理数量

-- 获取当前时间
local now = redis.call('TIME')
local milliseconds = now[1] * 1000 + math.floor(now[2] / 1000)

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', milliseconds, 'LIMIT', 0, ARGV[1])
if (#ids > 0) then
    for _, id in ipairs(ids) do
        local mKey = KEYS[3]..id
        -- 判断消息结构是否存在
        local found = redis.call('EXISTS', mKey)
        if (found == 1) then
            local uuid = redis.call('HGET', mKey, 'uuid')
            if (uuid ~= nil and uuid ~= '') then
                local nKey = KEYS[3]..uuid
                redis.call('RPUSH', KEYS[2], nKey)
                redis.call('RENAME', mKey, nKey)
            end
        end
        redis.call('ZREM', KEYS[1], id)
    end
end