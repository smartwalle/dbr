package dbr

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type UniversalClient interface {
	redis.UniversalClient

	Fetch(ctx context.Context, key string, fn func(context.Context) ([]byte, error), opts ...FetchOption) (value []byte, err error)
}

type Client struct {
	redis.UniversalClient
}

func New(addr, password string, db, poolSize, minIdleConns int) (UniversalClient, error) {
	var opt = &redis.Options{}
	opt.Addr = addr
	opt.Password = password
	opt.DB = db
	opt.PoolSize = poolSize
	opt.MinIdleConns = minIdleConns
	return NewWithOption(opt)
}

func NewWithOption(opts *redis.Options) (UniversalClient, error) {
	var rClient = redis.NewClient(opts)

	if _, err := rClient.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}

	return &Client{UniversalClient: rClient}, nil
}
