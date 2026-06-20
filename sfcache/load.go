package sfcache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// ErrNotFound 表示数据源中不存在该 key。fn 返回该错误时，Load 会写入 emptyKey 作为负面缓存。
var ErrNotFound = errors.New("not found")

// ErrStaleResult 表示本次加载结果已经失效，不能再写入缓存。
var ErrStaleResult = errors.New("stale result")

// loadGroup 合并同一进程内同一 client/key 的并发加载请求，降低 Redis 锁和数据源压力。
var loadGroup singleflight.Group

const (
	// 默认值要求 LockTimeout 覆盖 fn 执行和 Redis 写入两个阶段。
	defaultEmptyExpiration = time.Minute * 5
	defaultRetryDelay      = time.Millisecond * 50
	defaultLoadTimeout     = time.Second * 2
	defaultWriteTimeout    = time.Second
)

// Options 定义 Load 的缓存、等待和超时策略。
type Options struct {
	// Expiration 真实数据在 Redis 中的过期时间。零值表示不过期。
	Expiration time.Duration

	// EmptyExpiration 空结果在 Redis 中的过期时间。fn 返回 ErrNotFound 时会写入 emptyKey。
	EmptyExpiration time.Duration

	// RetryDelay 获取锁失败后的重试间隔。
	RetryDelay time.Duration

	// LoadTimeout fn 从数据源加载数据的超时时间。
	LoadTimeout time.Duration

	// WriteTimeout 写入真实缓存或 emptyKey 的超时时间。
	WriteTimeout time.Duration

	// LockTimeout 分布式锁的租约时间，应覆盖 LoadTimeout 和 WriteTimeout。
	// 如果配置值小于 LoadTimeout+WriteTimeout，会被提升到该最小值。
	LockTimeout time.Duration

	// WaitTimeout 等待其它加载者写入缓存的最长时间。
	// 零值时默认等到 LockTimeout 后再多等待一个 RetryDelay 周期。
	WaitTimeout time.Duration
}

// Option 修改 Load 的配置。
type Option func(opts *Options)

// WithExpiration 设置真实数据在 Redis 中的过期时间。
func WithExpiration(expiration time.Duration) Option {
	return func(opts *Options) {
		opts.Expiration = expiration
	}
}

// WithEmptyExpiration 设置空结果负面缓存的过期时间。
func WithEmptyExpiration(expiration time.Duration) Option {
	return func(opts *Options) {
		opts.EmptyExpiration = expiration
	}
}

// WithRetryDelay 设置等待其它加载者时的轮询间隔。
func WithRetryDelay(delay time.Duration) Option {
	return func(opts *Options) {
		opts.RetryDelay = delay
	}
}

// WithLoadTimeout 设置 fn 的加载超时时间。
func WithLoadTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.LoadTimeout = timeout
	}
}

// WithWriteTimeout 设置写入缓存结果的超时时间。
func WithWriteTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.WriteTimeout = timeout
	}
}

// WithLockTimeout 设置分布式锁租约时间。
// 小于 LoadTimeout+WriteTimeout 的值会被提升到该最小值。
func WithLockTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.LockTimeout = timeout
	}
}

// WithWaitTimeout 设置等待其它加载者写入缓存的最长时间。
func WithWaitTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.WaitTimeout = timeout
	}
}

// releaseScript 原子释放锁。
//
// KEYS[1]: lockKey，分布式锁 key。
// ARGV[1]: lockValue，当前加载者持有的锁 token。
var releaseScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

// setValueScript 在确认加载结果仍有效后写入真实缓存。
//
// KEYS[1]: lockKey，分布式锁 key。
// KEYS[2]: key，真实缓存 key。
// KEYS[3]: emptyKey，空结果缓存 key。
// ARGV[1]: lockValue，当前加载者持有的锁 token。
// ARGV[2]: value，要写入真实缓存的字节内容。
// ARGV[3]: expirationMillis，真实缓存过期时间，单位毫秒；小于等于 0 表示不过期。
var setValueScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			if tonumber(ARGV[3]) > 0 then
				redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
			else
				redis.call("SET", KEYS[2], ARGV[2])
			end
			redis.call("DEL", KEYS[3])
			return 1
		else
			return 0
		end
	`)

// setEmptyScript 在确认加载结果仍有效后写入 emptyKey。
//
// KEYS[1]: lockKey，分布式锁 key。
// KEYS[2]: key，真实缓存 key。
// KEYS[3]: emptyKey，空结果缓存 key。
// ARGV[1]: lockValue，当前加载者持有的锁 token。
// ARGV[2]: emptyValue，写入 emptyKey 的标记值。
// ARGV[3]: expirationMillis，emptyKey 过期时间，单位毫秒；小于等于 0 表示不过期。
var setEmptyScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			redis.call("DEL", KEYS[2])
			if tonumber(ARGV[3]) > 0 then
				redis.call("SET", KEYS[3], ARGV[2], "PX", ARGV[3])
			else
				redis.call("SET", KEYS[3], ARGV[2])
			end
			return 1
		else
			return 0
		end
	`)

// Load 从 Redis 缓存中加载数据，如果缓存未命中，则通过 fn 从数据源获取数据并写入缓存。
// 它同时处理两类常见缓存问题：
//  1. 缓存击穿：热点 key 过期或首次加载时，大量请求同时未命中缓存；Load 会合并并发加载，只让一个请求回源。
//  2. 缓存穿透：某个 key 在数据源中明确不存在，但被反复请求；fn 返回 ErrNotFound 时，Load 会写入 emptyKey 作为负面缓存。
//
// 该函数具有以下特性：
//  1. Single-Flight (并发抑制): 当多个请求同时加载同一个缺失的 key 时，只有一个请求会执行 fn 函数，
//     其他请求会等待并尝试从缓存中获取第一个请求加载的结果，从而防止缓存击穿。
//  2. 空结果缓存: 当 fn 返回 ErrNotFound 时，会写入 emptyKey 以防止缓存穿透。
//  3. 重试与超时: 支持配置加载、写入、锁租约和等待超时。
//
// fn 应区分“数据不存在”和“临时错误”：只有返回 ErrNotFound 才会写入负面缓存。
func Load(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts ...Option) (value []byte, err error) {
	loadOpts := newOptions(opts...)

	// 先用本地 singleflight 合并同一进程内的并发请求，避免大量 goroutine 同时竞争 Redis 锁。
	resultCh := loadGroup.DoChan(loadGroupKey(client, key), func() (any, error) {
		return load(ctx, client, key, fn, loadOpts)
	})

	select {
	case <-ctx.Done():
		// DoChan 允许等待者遵守自己的 ctx；底层加载仍会继续服务其它等待者。
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.([]byte), nil
	}
}

func loadGroupKey(client any, key string) string {
	// client 指针参与 key，避免同进程内不同 Redis client/DB 的同名 key 被错误合并。
	return fmt.Sprintf("%p:%s", client, key)
}

// newOptions 应用用户配置并填充默认值。
func newOptions(opts ...Option) *Options {
	var loadOpts = &Options{}
	for _, opt := range opts {
		if opt != nil {
			opt(loadOpts)
		}
	}
	if loadOpts.EmptyExpiration <= 0 {
		loadOpts.EmptyExpiration = defaultEmptyExpiration
	}
	if loadOpts.RetryDelay <= 0 {
		loadOpts.RetryDelay = defaultRetryDelay
	}
	if loadOpts.LoadTimeout <= 0 {
		loadOpts.LoadTimeout = defaultLoadTimeout
	}
	if loadOpts.WriteTimeout <= 0 {
		loadOpts.WriteTimeout = defaultWriteTimeout
	}

	// 锁租约必须覆盖“加载 + 写入”两个阶段，否则 fn 成功后写缓存前锁可能已经过期。
	minLockTimeout := loadOpts.LoadTimeout + loadOpts.WriteTimeout
	if loadOpts.LockTimeout <= 0 || loadOpts.LockTimeout < minLockTimeout {
		loadOpts.LockTimeout = minLockTimeout
	}
	if loadOpts.WaitTimeout <= 0 {
		// 多等待一个 RetryDelay，给等待者在锁租约结束附近再读一次缓存的机会。
		loadOpts.WaitTimeout = loadOpts.LockTimeout + loadOpts.RetryDelay
	}
	return loadOpts
}

// load 是 Load 的跨进程并发控制实现。它先读真实缓存和 emptyKey，
// 未命中时尝试获取 Redis 锁；未获取到锁的请求会轮询等待其它加载者写入结果。
func load(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) ([]byte, error), opts *Options) (value []byte, err error) {
	var lockKey = strings.Join([]string{key, "lock"}, ":")
	var emptyKey = strings.Join([]string{key, "empty"}, ":")

	var waitCtx = ctx
	var waitCancel context.CancelFunc
	if opts.WaitTimeout > 0 {
		waitCtx, waitCancel = context.WithTimeout(ctx, opts.WaitTimeout)
		defer waitCancel()
	}

	var lockValue = uuid.New().String()
	for {
		// 每轮都先读缓存，避免锁释放和缓存写入之间的短暂时序差导致重复加载。
		var found bool
		if value, found, err = getCached(ctx, client, key, emptyKey); err != nil {
			return value, err
		}
		if found {
			return value, nil
		}

		if err = ctx.Err(); err != nil {
			return nil, err
		}

		// SetNX 成功的请求负责回源加载；失败的请求进入等待轮询。
		var locked bool
		if locked, err = client.SetNX(ctx, lockKey, lockValue, opts.LockTimeout).Result(); err != nil {
			return nil, err
		}
		if locked {
			return loadLocked(ctx, client, key, emptyKey, lockKey, lockValue, fn, opts)
		}

		if err = waitCtx.Err(); err != nil {
			return nil, err
		}

		// 未拿到锁时按 RetryDelay 等待，下一轮会先检查缓存，仍未命中才再次竞争锁。
		if err = sleep(waitCtx, opts.RetryDelay); err != nil {
			return nil, err
		}
	}
}

// getCached 返回真实缓存命中结果；如果 emptyKey 存在，则返回 ErrNotFound。
func getCached(ctx context.Context, client redis.UniversalClient, key string, emptyKey string) ([]byte, bool, error) {
	// 真实缓存优先级高于 emptyKey；写入真实值时会删除 emptyKey。
	value, err := client.Get(ctx, key).Bytes()
	if err == nil {
		// found=true 表示已经拿到真实缓存，调用方可以直接返回 value。
		return value, true, nil
	}
	if !errors.Is(err, redis.Nil) {
		// Redis 读取异常不能当作缓存未命中，否则会绕过缓存保护直接回源。
		return value, false, err
	}

	// 真实缓存不存在时再检查 emptyKey。emptyKey 只表示“数据源确认不存在”，
	// 不表示加载中；加载中的状态由 lockKey 表达。
	var exists int64
	if exists, err = client.Exists(ctx, emptyKey).Result(); err != nil {
		return nil, false, err
	}
	if exists > 0 {
		// 命中负面缓存，调用方不应再执行 fn。
		return nil, false, ErrNotFound
	}

	// 真实缓存和负面缓存都不存在，调用方需要继续竞争加载权。
	return nil, false, nil
}

// loadLocked 在持有 Redis 锁后执行 fn，并根据结果写入真实缓存或 emptyKey。
func loadLocked(ctx context.Context, client redis.UniversalClient, key string, emptyKey string, lockKey string, lockValue string, fn func(context.Context) ([]byte, error), opts *Options) (value []byte, err error) {
	defer func() {
		// 使用不继承取消信号的 context 释放锁，避免上游 ctx 取消后锁只能等 TTL 过期。
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*2)
		defer releaseCancel()
		_ = releaseScript.Run(releaseCtx, client, []string{lockKey}, lockValue).Err()
	}()

	var found bool
	// 拿到锁后再 double-check，处理其它加载者刚刚写入缓存的竞态。
	if value, found, err = getCached(ctx, client, key, emptyKey); err != nil {
		return value, err
	}
	if found {
		return value, nil
	}

	var loadCancel context.CancelFunc
	loadCtx, loadCancel := context.WithTimeout(ctx, opts.LoadTimeout)
	defer loadCancel()

	// 只有持锁者会执行 fn；其它进程或 goroutine 会在 load 的等待循环里轮询结果。
	if value, err = fn(loadCtx); err != nil {
		if errors.Is(err, ErrNotFound) {
			// 只有明确不存在才写负面缓存；临时错误直接返回，避免把故障缓存成空结果。
			if err = setEmpty(ctx, client, key, emptyKey, lockKey, lockValue, opts.EmptyExpiration, opts.WriteTimeout); err != nil {
				return nil, err
			}
			return nil, ErrNotFound
		}
		// 临时错误不写入任何缓存，避免把短暂故障扩散到后续请求。
		return value, err
	}

	if err = loadCtx.Err(); err != nil {
		// fn 如果忽略 ctx 并在超时后才返回，其结果不能再写入缓存。
		return nil, err
	}

	if value == nil {
		// nil 表示空字节值的真实结果；是否“不存在”必须由 ErrNotFound 表达。
		value = []byte{}
	}

	// 成功加载后写真实缓存，并清理可能存在的 emptyKey，避免旧负面缓存继续生效。
	if err = setValue(ctx, client, key, emptyKey, lockKey, lockValue, value, opts.Expiration, opts.WriteTimeout); err != nil {
		return value, err
	}
	return value, nil
}

// setValue 在确认当前加载结果仍有效后写入真实缓存，并删除 emptyKey。
func setValue(ctx context.Context, client redis.UniversalClient, key string, emptyKey string, lockKey string, lockValue string, value []byte, expiration time.Duration, timeout time.Duration) error {
	// 写入也要有独立超时；LockTimeout 会默认覆盖 LoadTimeout+WriteTimeout。
	writeCtx, writeCancel := context.WithTimeout(ctx, timeout)
	defer writeCancel()

	// 脚本会先校验 lockValue，确认当前加载结果仍然属于最新持有者，再写 key 并删除 emptyKey。
	stored, err := setValueScript.Run(writeCtx, client, []string{lockKey, key, emptyKey}, lockValue, value, durationMilliseconds(expiration)).Int()
	if err != nil {
		return err
	}
	if stored != 1 {
		// 校验失败说明结果已经过时，不能覆盖其它加载者可能写入的新结果。
		return ErrStaleResult
	}
	return nil
}

// setEmpty 在确认当前加载结果仍有效后写入 emptyKey，并删除真实缓存。
func setEmpty(ctx context.Context, client redis.UniversalClient, key string, emptyKey string, lockKey string, lockValue string, expiration time.Duration, timeout time.Duration) error {
	// 写 emptyKey 与写真实缓存一样需要受 WriteTimeout 约束。
	writeCtx, writeCancel := context.WithTimeout(ctx, timeout)
	defer writeCancel()

	// 脚本会先校验 lockValue，再删除真实缓存并写入 emptyKey，保证空结果和真实结果不会同时有效。
	stored, err := setEmptyScript.Run(writeCtx, client, []string{lockKey, key, emptyKey}, lockValue, "1", durationMilliseconds(expiration)).Int()
	if err != nil {
		return err
	}
	if stored != 1 {
		// 当前加载结果已经过时，不能再把空结果写入缓存。
		return ErrStaleResult
	}
	return nil
}

// durationMilliseconds 将 Redis 过期时间转换成毫秒，保留小于 1ms 的正值。
func durationMilliseconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	if duration < time.Millisecond {
		return 1
	}
	return int64(duration / time.Millisecond)
}

// sleep 等待指定间隔，并在 ctx 取消时提前返回。
func sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
