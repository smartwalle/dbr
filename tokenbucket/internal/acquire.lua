-- KEYS[1] - key 名称
-- ARGV[1] - 令牌桶容量
-- ARGV[2] - 每秒生成令牌数量
redis.replicate_commands()

local key = KEYS[1]
local capacity = tonumber(ARGV[1]) -- 令牌桶容量
local rate = tonumber(ARGV[2]) -- 每秒生成令牌数量
local now = redis.call('TIME')[1] -- 当前时间

-- 获取桶中剩余的令牌数量
local tokens = tonumber(redis.call('HGET', key, 'tokens') or capacity)
-- 获取上次填充令牌桶的时间
local refillTime = tonumber(redis.call('HGET', key, 'refill_time') or now)
-- 计算当前时间和上次填充令牌的时间差
local elapsedTime = now - refillTime
-- 计算出当前令牌桶剩余数量
local newTokens = math.min(capacity, tokens + elapsedTime * rate)

if newTokens >= 1 then
    -- 更新令牌信息
    redis.call('HSET', key, 'tokens', newTokens - 1, 'refill_time', now)
    return true
end
return false