package tokenbucket

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	client redis.UniversalClient

	key      string
	capacity int // 令牌桶容量
	rate     int // 每秒生成令牌数量
}

func New(client redis.UniversalClient, key string, capacity, rate int) *Limiter {
	var limiter = &Limiter{}
	limiter.client = client
	limiter.key = key
	limiter.capacity = capacity
	limiter.rate = rate
	return limiter
}

func (limiter *Limiter) Allow(ctx context.Context, paths ...string) bool {
	var fullpath = make([]string, 1, len(paths)+1)
	fullpath[0] = limiter.key
	if len(paths) > 0 {
		fullpath = append(fullpath, paths...)
	}

	var keys = []string{
		strings.Join(fullpath, ":"),
	}
	var args = []interface{}{
		time.Now().Unix(),
		limiter.capacity,
		limiter.rate,
		1,
	}
	value, err := acquireScript.Run(ctx, limiter.client, keys, args...).Bool()
	if err != nil {
		return false
	}
	return value
}

var acquireScript = redis.NewScript(`
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
	local tokens = tonumber(redis.call('HGET', key, 'tokens')) or capacity
	-- 获取上次填充令牌桶的时间
	local refillTime = tonumber(redis.call('HGET', key, 'refill_time')) or now
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
`)
