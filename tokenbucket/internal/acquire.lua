-- KEYS[1] - key 名称
-- ARGV[1] - 当前时间(秒)
-- ARGV[2] - 令牌桶容量
-- ARGV[3] - 每秒生成令牌数量
-- ARGV[4] - 消耗令牌数量
-- redis.replicate_commands()

local key = KEYS[1]
local now = tonumber(ARGV[1]) -- 当前时间(秒)
local capacity = tonumber(ARGV[2]) -- 令牌桶容量
local rate = tonumber(ARGV[3]) -- 每秒生成令牌数量
local requested = tonumber(ARGV[4]) -- 消耗令牌数量

-- 获取桶中剩余的令牌数量
local tokens = tonumber(redis.call('HGET', key, 'tokens') or capacity)
-- 获取上次填充令牌桶的时间
local refillTime = tonumber(redis.call('HGET', key, 'refill_time') or now)
-- 计算当前时间和上次填充令牌的时间差
local elapsedTime = now - refillTime
-- 计算出当前令牌桶剩余数量
local refillTokens = math.min(capacity, tokens + elapsedTime * rate)
-- 是否有足够的令牌
local allowed = refillTokens >= requested

if allowed then
    -- 更新令牌信息
    redis.call('HSET', key, 'tokens', refillTokens - requested, 'refill_time', now)
end

-- 设置 key 的过期时间，为令牌桶填充满需要的时间乘以3
redis.call('EXPIRE', key, (capacity / rate) * 3)

return allowed