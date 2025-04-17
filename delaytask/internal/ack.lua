-- KEYS[1] - 处理中队列
-- KEYS[2] - MessageKey(uuid)

local mKey = KEYS[2]
-- 从[处理中队列]获取消息的分值，主要用于判断该消息是否还存在于[处理中队列]中
local score = redis.call('ZSCORE', KEYS[1], mKey)
if (not score) then
    return ''
end

-- 获取消息 uuid
local uuid = redis.call('HGET', mKey, 'uuid')
-- 从[处理中队列]删除
redis.call('ZREM', KEYS[1], mKey)
-- 删除消息结构
redis.call('DEL', mKey)
return uuid