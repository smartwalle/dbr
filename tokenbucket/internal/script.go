package internal

import (
	_ "embed"
	"github.com/redis/go-redis/v9"
)

//go:embed acquire.lua
var acquireScript string

// AcquireScript 获取令牌
var AcquireScript = redis.NewScript(acquireScript)
