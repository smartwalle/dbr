package redlock

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockFailed  = errors.New("failed to acquire lock")
	ErrLockNotHeld = errors.New("lock not held")
)

// Mutex 分布式互斥锁
type Mutex struct {
	client       redis.UniversalClient
	key          string
	token        string
	options      *Options
	acquiredTime time.Time // 获得锁时间
}

// Options 分布式锁选项
type Options struct {
	// Duration 锁的持续时间
	Duration time.Duration

	// Timeout 获取锁超时时间，超过此时间后不再重试并返回错误，小于等于0则表示不会进行等待
	Timeout time.Duration

	// RetryDelay 重试间隔
	RetryDelay time.Duration
}

// Option 分布式锁选项函数
type Option func(*Options)

// WithDuration 设置锁持续时间
func WithDuration(duration time.Duration) Option {
	return func(opts *Options) {
		opts.Duration = duration
	}
}

// WithRetryDelay 设置重试间隔
func WithRetryDelay(delay time.Duration) Option {
	return func(opts *Options) {
		if delay > 0 {
			opts.RetryDelay = delay
		}
	}
}

// WithTimeout 设置获取锁超时时间，即 Lock 方法的最大等待时间，如果小于等于0，获取锁没有成功也不会进行等待
func WithTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.Timeout = timeout
	}
}

// defaultOptions 默认选项
func defaultOptions() *Options {
	return &Options{
		Duration:   2 * time.Second,        // 默认持续时间2秒
		Timeout:    5 * time.Second,        // 默认最大等待5秒
		RetryDelay: 100 * time.Millisecond, // 默认重试延迟100毫秒
	}
}

// New 创建分布式锁
func New(client redis.UniversalClient, key string, opts ...Option) *Mutex {
	var options = defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	return &Mutex{
		client:  client,
		key:     key,
		options: options,
	}
}

// Lock 获取锁，直到成功或上下文取消
func (m *Mutex) Lock(ctx context.Context) (err error) {
	// 没有设置“获取锁超时时间”，则只尝试一次
	if m.options.Timeout <= 0 {
		return m.TryLock(ctx)
	}

	var acquireCtx, acquireCancel = context.WithTimeout(ctx, m.options.Timeout)
	defer acquireCancel()

	for {
		select {
		case <-acquireCtx.Done():
			return ErrLockFailed
		default:
			if err = m.TryLock(acquireCtx); err == nil {
				return nil
			}
			if !errors.Is(err, ErrLockFailed) {
				return err
			}
			if err = Sleep(acquireCtx, m.options.RetryDelay); err != nil {
				return err
			}
		}
	}
}

func Sleep(ctx context.Context, duration time.Duration) error {
	var timer = time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// TryLock 尝试获取锁，会立即返回结果
func (m *Mutex) TryLock(ctx context.Context) error {
	var token = uuid.New().String()
	acquired, err := acquireLock(ctx, m.client, m.key, token, m.options.Duration)
	if err != nil {
		return err
	}
	if !acquired {
		return ErrLockFailed
	}
	m.token = token
	m.acquiredTime = time.Now()
	return nil
}

// Unlock 释放锁
func (m *Mutex) Unlock(ctx context.Context) error {
	success, err := releaseLock(ctx, m.client, m.key, m.token)
	if err != nil {
		return err
	}
	if !success {
		return ErrLockNotHeld
	}
	return nil
}

// Extend 续约锁
func (m *Mutex) extend(ctx context.Context) error {
	success, err := extendLock(ctx, m.client, m.key, m.token, m.options.Duration)
	if err != nil {
		return err
	}
	if !success {
		return ErrLockNotHeld
	}
	return nil
}

// Lua scripts
var (
	acquireScript = redis.NewScript(`
		if redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX") then
			return 1
		else
			return 0
		end
	`)

	releaseScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	extendScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			redis.call("PEXPIRE", KEYS[1], ARGV[2])
			return 1
		else
			return 0
		end
	`)
)

// acquireLock 获取锁
func acquireLock(ctx context.Context, client redis.UniversalClient, key, token string, duration time.Duration) (bool, error) {
	result, err := acquireScript.Run(ctx, client, []string{key}, token, duration.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// releaseLock 释放锁
func releaseLock(ctx context.Context, client redis.UniversalClient, key, token string) (bool, error) {
	result, err := releaseScript.Run(ctx, client, []string{key}, token).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// extendLock 续约锁
func extendLock(ctx context.Context, client redis.UniversalClient, key, token string, duration time.Duration) (bool, error) {
	result, err := extendScript.Run(ctx, client, []string{key}, token, duration.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}
