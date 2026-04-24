package sfcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrHitPlaceholder = errors.New("hit placeholder")

type Options struct {
	// Expiration 数据在 Redis 中的过期时间。
	Expiration time.Duration

	// Placeholder 占位符。当缓存未命中时，会先在 Redis 中写入该占位符，
	// 用于防止缓存穿透。
	Placeholder []byte

	// PlaceholderExpiration 占位符在 Redis 中的过期时间。
	PlaceholderExpiration time.Duration

	// MaxAttempts 获取分布式锁的最大尝试次数。
	MaxAttempts int

	// RetryDelay 获取锁失败后的重试间隔。
	RetryDelay time.Duration

	// LoadTimeout 加载超时时间，同时也用作分布式锁的租约时间。
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

// Load 从 Redis 缓存中加载数据，如果缓存未命中，则通过 fn 函数从数据源获取数据并写入缓存。
//
// 该函数具有以下特性：
//  1. Single-Flight (并发抑制): 当多个请求同时加载同一个缺失的 key 时，只有一个请求会执行 fn 函数，
//     其他请求会等待并尝试从缓存中获取第一个请求加载的结果，从而防止缓存击穿。
//  2. 占位符机制: 支持写入占位符以防止缓存穿透。如果 fn 执行较慢，先占位的机制也能让后续请求感知到正在加载中。
//  3. 重试与超时: 在等待锁的过程中支持配置重试次数和延迟。
func Load(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts ...Option) (value []byte, err error) {
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
	return load(ctx, client, key, fn, loadOpts)
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

// load 是 Load 函数的内部实现，包含了核心的缓存加载逻辑。
func load(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts *Options) (value []byte, err error) {
	// 从 redis 加载数据
	// 如果返回了非 redis.Nil 错误，则直接返回
	if value, err = client.Get(ctx, key).Bytes(); err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在
	if err == nil {
		// 检查是否为加载中的占位符
		if len(value) > 0 && len(opts.Placeholder) > 0 && bytes.Equal(value, opts.Placeholder) {
			// 如果是占位符，不直接返回，继续往下执行，通过“加锁”环节进行等待
			// return value, ErrHitPlaceholder
		} else {
			// 返回数据
			return value, nil
		}
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

	// 此 context 仅用于 fn 中，其它 redis 相关的操作正常使用 ctx
	var loadCtx = ctx

	if locked {
		var loadCancel func()
		loadCtx, loadCancel = context.WithTimeout(ctx, opts.LoadTimeout)
		defer loadCancel()

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
		// 检查是否为占位符
		if len(value) > 0 && len(opts.Placeholder) > 0 && bytes.Equal(value, opts.Placeholder) {
			return value, ErrHitPlaceholder
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
		if value, err = fn(loadCtx); err != nil {
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
