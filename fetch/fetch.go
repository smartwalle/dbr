package fetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Options struct {
	Expiration time.Duration

	Placeholder           []byte
	PlaceholderExpiration time.Duration

	MaxAttempts int
	RetryDelay  time.Duration

	LoadTimeout time.Duration
}

type Option func(opts *Options)

func WithExpiration(expiration time.Duration) Option {
	return func(opts *Options) {
		opts.Expiration = expiration
	}
}

func WithPlaceholder(placeholder []byte) Option {
	return func(opts *Options) {
		opts.Placeholder = placeholder
	}
}

func WithPlaceholderExpiration(expiration time.Duration) Option {
	return func(opts *Options) {
		opts.PlaceholderExpiration = expiration
	}
}

func WithRetryDelay(delay time.Duration) Option {
	return func(opts *Options) {
		if delay <= 0 {
			delay = time.Millisecond * 50
		}
		opts.RetryDelay = delay
	}
}

func WithMaxAttempts(attempts int) Option {
	return func(opts *Options) {
		if attempts <= 0 {
			attempts = 3
		}
		opts.MaxAttempts = attempts
	}
}

func WithLoadTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
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

func Do(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts ...Option) (value []byte, err error) {
	var loadOpts = &Options{}
	loadOpts.Placeholder = []byte("-")
	loadOpts.PlaceholderExpiration = time.Minute * 5
	loadOpts.MaxAttempts = 3
	loadOpts.RetryDelay = time.Millisecond * 50
	loadOpts.LoadTimeout = time.Second * 2
	for _, opt := range opts {
		if opt != nil {
			opt(loadOpts)
		}
	}
	if loadOpts.LoadTimeout < loadOpts.RetryDelay*time.Duration(loadOpts.MaxAttempts) {
		loadOpts.LoadTimeout += loadOpts.RetryDelay * time.Duration(loadOpts.MaxAttempts)
	}
	return do(ctx, client, key, fn, loadOpts)
}

func delay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	var timer = time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func do(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts *Options) (value []byte, err error) {
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
	var lockValue = uuid.New().String()
	var attempt = 1
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
			// 达到最大可尝试次数之后依然没有加锁成功，中止当前循环，执行后续“再次从 redis 加载数据”的逻辑
			break
		}

		if opts.RetryDelay > 0 {
			if err = delay(ctx, opts.RetryDelay); err != nil {
				return nil, err
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
		if err = client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration).Err(); err != nil {
			return value, err
		}

		// 从“数据源”读取数据
		if value, err = fn(ctx); err != nil {
			// 从“数据源”读取数据返回 err，写入“占位符”
			//if err = client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration).Err(); err != nil {
			//	return value, err
			//}
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
