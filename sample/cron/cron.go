package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
	"time"
)

func main() {
	var pool = dbr.NewRedis("127.0.0.1:6379", 10, 2, redis.DialDatabase(15))
	var pool2 = dbr.NewRedis("127.0.0.1:6380", 10, 2, redis.DialDatabase(15))
	var pool3 = dbr.NewRedis("127.0.0.1:6381", 10, 2, redis.DialDatabase(15))
	var rs = dbr.NewRedSync(pool, pool2, pool3)

	var key = "aa"

	var c = dbr.NewCron(key, pool, dbr.WithCronRedSync(rs))

	c.Add(fmt.Sprintf("%s1", key), "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})
	c.Add(fmt.Sprintf("%s2", key), "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})
	select {}

}
