package dbr

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type UniversalClient interface {
	redis.UniversalClient

	GetBlock(ctx context.Context, key string, opts ...BlockOption) (bool, string, error)
}

type Client struct {
	*redis.Client
}

func New(addr, password string, db, poolSize, minIdleConn int) (UniversalClient, error) {
	var opt = &redis.Options{}
	opt.Addr = addr
	opt.Password = password
	opt.DB = db
	opt.PoolSize = poolSize
	opt.MinIdleConns = minIdleConn
	return NewWithOption(opt)
}

func NewWithOption(opt *redis.Options) (UniversalClient, error) {
	var rClient = redis.NewClient(opt)

	if _, err := rClient.Ping(context.TODO()).Result(); err != nil {
		return nil, err
	}

	return &Client{Client: rClient}, nil
}
