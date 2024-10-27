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

	MaxRetries int
	RetryDelay time.Duration

	LoadDataTimeout time.Duration
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

func WithLoadDataTimeout(timeout time.Duration) FetchOption {
	return func(opts *fetchOptions) {
		if timeout <= 0 {
			timeout = time.Second * 2
		}
		opts.LoadDataTimeout = timeout
	}
}

func Fetch(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) (string, time.Duration, error), opts ...FetchOption) (value string, err error) {
	var nOpts = &fetchOptions{}
	nOpts.Placeholder = "-"
	nOpts.PlaceholderExpiration = time.Minute * 5
	nOpts.MaxRetries = 3
	nOpts.RetryDelay = time.Millisecond * 50
	nOpts.LoadDataTimeout = time.Second * 2
	for _, opt := range opts {
		if opt != nil {
			opt(nOpts)
		}
	}
	if nOpts.LoadDataTimeout < nOpts.RetryDelay*time.Duration(nOpts.MaxRetries) {
		nOpts.LoadDataTimeout += nOpts.RetryDelay * time.Duration(nOpts.MaxRetries)
	}
	return fetch(ctx, client, key, fn, nOpts)
}

func fetch(ctx context.Context, client redis.UniversalClient, key string, fn func(context.Context) (string, time.Duration, error), opts *fetchOptions) (value string, err error) {
	// 从 redis 加载数据
	// 如果返回了非 redis.Nil 错误，则直接返回
	if value, err = client.Get(ctx, key).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在
	if err == nil {
		// 如果 value 的值为“占位符”，则返回 redis.Nil 错误
		if value == opts.Placeholder {
			return value, redis.Nil
		}
		// 返回数据
		return value, nil
	}

	// 当 err 为 redis.Nil 时，表示在 redis 中不存在该 key，所以继续往下执行

	// 添加用于从“数据源”获取数据锁
	var lockKey = fmt.Sprintf("%s:lock", key)
	var runTimes = 0
	var locked = false
	for runTimes <= opts.MaxRetries {
		if locked, _ = client.SetNX(ctx, lockKey, fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixMilli()), opts.LoadDataTimeout).Result(); locked {
			break
		}
		if runTimes < opts.MaxRetries {
			time.Sleep(opts.RetryDelay)
		}
		runTimes += 1
	}

	if locked {
		// 释放锁
		defer func() {
			client.Del(ctx, lockKey)
		}()
	}

	// 添加用于从“数据源”获取数据锁成功或失败，都再次从 redis 加载数据
	// 如果返回了非 redis.Nil 错误，则直接返回
	if value, err = client.Get(ctx, key).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return value, err
	}
	// 如果 err 为 nil，表示 key 在 redis 中已经存在
	if err == nil {
		// 如果 value 的值为“占位符”，则返回 redis.Nil 错误
		if value == opts.Placeholder {
			return value, redis.Nil
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
		var expiration time.Duration
		if value, expiration, err = fn(ctx); err != nil {
			// 从“数据源”读取数据返回 err，写入“占位符”
			//client.SetNX(ctx, key, opts.Placeholder, opts.PlaceholderExpiration)
			return value, err
		}

		// 从“数据源”读取数据成功，将数据写入缓存
		if err = client.Set(ctx, key, value, expiration).Err(); err != nil {
			return value, err
		}
		return value, nil
	}
	return value, redis.Nil
}
