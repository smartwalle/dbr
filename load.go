package dbr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"time"
)

func (c *Client) Load(ctx context.Context, key string, fn func(context.Context) ([]byte, error), opts ...LoadOption) (value []byte, err error) {
	return Load(ctx, c, key, fn, opts...)
}

type loadOptions struct {
	Expiration time.Duration

	Placeholder           []byte
	PlaceholderExpiration time.Duration

	MaxAttempts int
	RetryDelay  time.Duration

	LoadTimeout time.Duration
}

type LoadOption func(opts *loadOptions)

func WithExpiration(expiration time.Duration) LoadOption {
	return func(opts *loadOptions) {
		opts.Expiration = expiration
	}
}

func WithPlaceholder(placeholder []byte) LoadOption {
	return func(opts *loadOptions) {
		opts.Placeholder = placeholder
	}
}

func WithPlaceholderExpiration(expiration time.Duration) LoadOption {
	return func(opts *loadOptions) {
		opts.PlaceholderExpiration = expiration
	}
}

func WithRetryDelay(delay time.Duration) LoadOption {
	return func(opts *loadOptions) {
		if delay <= 0 {
			delay = time.Millisecond * 50
		}
		opts.RetryDelay = delay
	}
}

func WithMaxAttempts(attempts int) LoadOption {
	return func(opts *loadOptions) {
		if attempts <= 0 {
			attempts = 3
		}
		opts.MaxAttempts = attempts
	}
}

func WithLoadTimeout(timeout time.Duration) LoadOption {
	return func(opts *loadOptions) {
		if timeout <= 0 {
			timeout = time.Second * 2
		}
		opts.LoadTimeout = timeout
	}
}

var releaseLockScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func Load(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts ...LoadOption) (value []byte, err error) {
	var fetchOpts = &loadOptions{}
	fetchOpts.Placeholder = []byte("-")
	fetchOpts.PlaceholderExpiration = time.Minute * 5
	fetchOpts.MaxAttempts = 3
	fetchOpts.RetryDelay = time.Millisecond * 50
	fetchOpts.LoadTimeout = time.Second * 2
	for _, opt := range opts {
		if opt != nil {
			opt(fetchOpts)
		}
	}
	if fetchOpts.LoadTimeout < fetchOpts.RetryDelay*time.Duration(fetchOpts.MaxAttempts) {
		fetchOpts.LoadTimeout += fetchOpts.RetryDelay * time.Duration(fetchOpts.MaxAttempts)
	}
	return load(ctx, client, key, fn, fetchOpts)
}

func load(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts *loadOptions) (value []byte, err error) {
	// 从 redis 加载数据
	// 如果返回了非 redis.Nil 错误，则直接返回
	if value, err = client.Get(ctx, key).Bytes(); err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在
	if err == nil {
		// 检查是否为加载中的占位符
		if len(value) > 0 && len(opts.Placeholder) > 0 && bytes.Equal(value, opts.Placeholder) {
			return nil, redis.Nil
		}
		// 返回数据
		return value, nil
	}

	// 当 err 为 redis.Nil 时，表示在 redis 中不存在该 key，所以继续往下执行

	// 添加用于从“数据源”获取数据锁
	var lockKey = fmt.Sprintf("%s:lock", key)
	var lockValue = fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixMicro())
	var attempt = 0
	var locked = false
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		if locked, err = client.SetNX(ctx, lockKey, lockValue, opts.LoadTimeout).Result(); err != nil {
			return value, err
		}
		if locked {
			break
		}

		attempt += 1
		if attempt > opts.MaxAttempts {
			break
		}

		if opts.RetryDelay > 0 {
			var timer = time.NewTimer(opts.RetryDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			}
		}
	}

	if locked {
		// 使用 Lua 脚本原子性释放锁
		defer func() {
			releaseLockScript.Run(ctx, client, []string{lockKey}, lockValue)
		}()
	}

	// 再次从 redis 加载数据
	// 如果返回了非 redis.Nil 错误，则直接返回
	if value, err = client.Get(ctx, key).Bytes(); err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在
	if err == nil {
		// 检查是否为加载中的占位符
		if len(value) > 0 && len(opts.Placeholder) > 0 && bytes.Equal(value, opts.Placeholder) {
			return nil, redis.Nil
		}
		// 返回数据
		return value, nil
	}

	// 当 err 为 redis.Nil 时，表示在 redis 中不存在该 key，所以继续往下执行

	// 添加用于从“数据源”获取数据锁成功
	if locked {
		// 写入“占位符”
		client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration)

		// 从“数据源”读取数据
		if value, err = fn(ctx); err != nil {
			// 从“数据源”读取数据返回 err，写入“占位符”
			//client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration)
			return value, err
		}

		// 从“数据源”读取数据成功，将数据写入缓存
		if err = client.Set(ctx, key, value, opts.Expiration).Err(); err != nil {
			return value, err
		}
		return value, nil
	}
	return value, redis.Nil
}
