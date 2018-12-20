package main

import (
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
)

func main() {
	var pool = dbr.NewRedis("192.168.1.99:6379", 10, 2, redis.DialDatabase(15))

	var sSess = pool.GetSession()
	defer sSess.Close()

	sSess.XADD("test_key", 0, "*", "email", "qq@qq.com", "user", "smartwalle")
}
