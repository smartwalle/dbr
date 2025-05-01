-- KEYS[1] - 处理中队列
-- KEYS[2] - MessageKey(uuid)

local runningKey = KEYS[1]
local messageKey = KEYS[2]

-- 从[处理中队列]获取消息的分值，主要用于判断该消息是否还存在于[处理中队列]中
local score = tonumber(redis.call('ZSCORE', runningKey, messageKey)) or 0
if score == 0 then
    return ''
end

-- 获取消息 uuid
local uuid = redis.call('HGET', messageKey, 'uuid')
-- 从[处理中队列]删除
redis.call('ZREM', runningKey, messageKey)
-- 删除消息结构
redis.call('DEL', messageKey)
return uuid