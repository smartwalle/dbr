package tokenbucket

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/tokenbucket/internal"
)

type Option func(limiter *Limiter)

func WithCapacity(capacity int) Option {
	return func(limiter *Limiter) {
		if capacity <= 0 {
			capacity = 10
		}
		limiter.capacity = capacity
	}
}

func WithRefillRate(rate int) Option {
	return func(limiter *Limiter) {
		if rate <= 0 {
			rate = 1
		}
		limiter.rate = rate
	}
}

type Limiter struct {
	client redis.UniversalClient
	key    string

	capacity int // 令牌桶容量
	rate     int // 每秒生成令牌数量
}

func New(client redis.UniversalClient, name string, opts ...Option) *Limiter {
	var limiter = &Limiter{}
	limiter.client = client
	limiter.key = fmt.Sprintf("dbr:tokenbucket:{%s}", name)
	limiter.capacity = 10
	limiter.rate = 1
	for _, opt := range opts {
		if opt != nil {
			opt(limiter)
		}
	}
	return limiter
}

func (limiter *Limiter) Allow(ctx context.Context) bool {
	var keys = []string{
		limiter.key,
	}
	var args = []interface{}{
		limiter.capacity,
		limiter.rate,
	}
	value, err := internal.AcquireScript.Run(ctx, limiter.client, keys, args...).Bool()
	if err != nil {
		return false
	}
	return value
}
