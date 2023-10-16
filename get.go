package dbr

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type blockOptions struct {
	BlockValue string
	BlockTime  time.Duration
	RetryDelay time.Duration
}

type BlockOption func(opt *blockOptions)

func WithBlockValue(blockValue string) BlockOption {
	return func(opt *blockOptions) {
		opt.BlockValue = blockValue
	}
}

func WithBlockTime(blockTime time.Duration) BlockOption {
	return func(opt *blockOptions) {
		opt.BlockTime = blockTime
	}
}

func WithRetryDelay(retryDelay time.Duration) BlockOption {
	return func(opt *blockOptions) {
		opt.RetryDelay = retryDelay
	}
}

// GetBlock 当 Redis 中不存在指定 key 时，会为该 key 指定一个特殊的 blockValue，并且设定过期时间，主要用于避免发生缓存穿透。
// 当第一个返回值为 true 时，表示该 key 已经被标记为无效 key 或者访问 Redis 发生错误，调用方应该终止后续操作，比如从数据库查询。
// 当第一个返回值为 false 时，表示该 key 本轮访问有效，调用方应该根据第二个返回值决定后续操作流程，比如直接返回或者从数据库查询并重写 Redis 中该 key 的值。
func (c *Client) GetBlock(ctx context.Context, key string, opts ...BlockOption) (bool, string, error) {
	var nOpts = &blockOptions{
		BlockValue: "block-null",
		BlockTime:  time.Minute * 5,
		RetryDelay: time.Millisecond * 200,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(nOpts)
		}
	}

	return c.getBlock(ctx, key, nOpts)
}

func (c *Client) getBlock(ctx context.Context, key string, opts *blockOptions) (bool, string, error) {
	var result = c.Get(ctx, key)
	var val, err = result.Result()
	if err != nil && err != redis.Nil {
		return true, "", err
	}

	// 如果该 key 没有数据，则尝试对其执行写入操作
	if err == redis.Nil {
		// 当从 redis 没有获取到数据的时候，写入 blockValue
		if c.SetNX(ctx, key, opts.BlockValue, opts.BlockTime).Val() {
			// 写入成功，直接返回不需要阻塞
			return false, "", nil
		}

		// 写入失败，则表示其它地方写入数据成功，延迟再次调用
		time.Sleep(opts.RetryDelay)
		return c.getBlock(ctx, key, opts)
	}

	if val == opts.BlockValue {
		// 该 key 有数据，并且数据等于 blockValue 的时候，返回阻塞
		return true, "", nil
	}

	return false, val, nil
}
