package dbr

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"time"
)

func (c *Client) Fetch(ctx context.Context, key string, fn func(context.Context) (string, time.Duration, error), opts ...FetchOption) (value string, err error) {
	return Fetch(ctx, c, key, fn, opts...)
}

type fetchOptions struct {
	Placeholder           string
	PlaceholderExpiration time.Duration

	MaxRetries   int
	RetryDelay   time.Duration
	RetryTimeout time.Duration
}

type FetchOption func(opts *fetchOptions)

func WithPlaceholder(placeholder string) FetchOption {
	return func(opts *fetchOptions) {
		opts.Placeholder = placeholder
	}
}

func WithPlaceholderExpiration(expiration time.Duration) FetchOption {
	return func(opts *fetchOptions) {
		opts.PlaceholderExpiration = expiration
	}
}

func WithRetryDelay(delay time.Duration) FetchOption {
	return func(opts *fetchOptions) {
		if delay <= 0 {
			delay = time.Millisecond * 50
		}
		opts.RetryDelay = delay
	}
}

func WithMaxRetries(retries int) FetchOption {
	return func(opts *fetchOptions) {
		if retries <= 0 {
			retries = 3
		}
		opts.MaxRetries = retries
	}
}

func WithRetryTimeout(timeout time.Duration) FetchOption {
	return func(opts *fetchOptions) {
		if timeout <= 0 {
			timeout = time.Second
		}
		opts.RetryTimeout = timeout
	}
}

func Fetch(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) (string, time.Duration, error), opts ...FetchOption) (value string, err error) {
	var nOpts = &fetchOptions{}
	nOpts.Placeholder = "-"
	nOpts.PlaceholderExpiration = time.Minute * 5
	nOpts.MaxRetries = 3
	nOpts.RetryDelay = time.Millisecond * 50
	nOpts.RetryTimeout = time.Second * 2
	for _, opt := range opts {
		if opt != nil {
			opt(nOpts)
		}
	}
	return fetch(ctx, client, key, fn, nOpts)
}

func fetch(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) (string, time.Duration, error), opts *fetchOptions) (value string, err error) {
	// 从缓存加载数据
	value, err = client.Get(ctx, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 value 的值为“占位符”，则直接返回
	if value == opts.Placeholder {
		return value, redis.Nil
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在，则直接返回
	if err == nil {
		return value, nil
	}

	// 添加用于从“数据源”获取数据锁
	var lockKey = fmt.Sprintf("%s:lock", key)
	var runTimes = 0
	var locked = false
	for runTimes <= opts.MaxRetries {
		if locked, _ = client.SetNX(ctx, lockKey, fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixMilli()), opts.RetryTimeout).Result(); locked {
			break
		}
		if runTimes < opts.MaxRetries {
			time.Sleep(opts.RetryDelay)
		}
		runTimes += 1
	}
	// 添加用于从“数据源”获取数据锁失败，直接返回
	if !locked {
		return value, redis.Nil
	}
	defer func() {
		client.Del(ctx, lockKey)
	}()

	// 添加用于从“数据源”获取数据锁成功，再次从缓存加载数据
	value, err = client.Get(ctx, key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 value 的值为“占位符”，则直接返回
	if value == opts.Placeholder {
		return value, redis.Nil
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在，则直接返回
	if err == nil {
		return value, nil
	}

	// 先写入“占位符”
	//client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration)

	// 如果 err 为 redis.Nil, 则从“数据源”读取数据
	// 从“数据源”读取数据
	value, expiration, err := fn(ctx)
	if err != nil {
		// 从“数据源”读取数据返回 err，写入“占位符”
		client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration)
		return value, err
	}

	// 从“数据源”读取数据成功，将数据写入缓存
	if err = client.Set(ctx, key, value, expiration).Err(); err != nil {
		return value, err
	}

	return value, nil
}
