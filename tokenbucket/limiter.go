package tokenbucket

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/tokenbucket/internal"
	"time"
)

type Limiter struct {
	client redis.UniversalClient

	capacity int // 令牌桶容量
	rate     int // 每秒生成令牌数量
}

func New(client redis.UniversalClient, capacity, rate int) *Limiter {
	var limiter = &Limiter{}
	limiter.client = client
	limiter.capacity = capacity
	limiter.rate = rate
	return limiter
}

func (limiter *Limiter) Allow(ctx context.Context, key string) bool {
	var keys = []string{
		fmt.Sprintf("dbr:tokenbucket:{%s}", key),
	}
	var args = []interface{}{
		time.Now().Unix(),
		limiter.capacity,
		limiter.rate,
		1,
	}
	value, err := internal.AcquireScript.Run(ctx, limiter.client, keys, args...).Bool()
	if err != nil {
		return false
	}
	return value
}
