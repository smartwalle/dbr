-- KEYS[1] - 就绪队列
-- KEYS[2] - 处理中队列
-- KEYS[3] - 消费者队列
-- ARGV[1] - 消费者id

local mKey = redis.call('LPOP', KEYS[1])
if (not mKey) then
    return ''
end
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
    return uuid
end
return ''