package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
	"time"
)

func main() {
	var pool = dbr.NewRedis("127.0.0.1:6379", 10, 2, redis.DialDatabase(15))
	var pool2 = dbr.NewRedis("192.168.1.77:6379", 10, 2, redis.DialDatabase(15))
	var rs = dbr.NewRedSync(pool, pool2)

	var key = "cc"

	var tm = dbr.NewTaskManager(key, pool, dbr.WithTaskRedSync(rs))

	tm.AddTask(fmt.Sprintf("%s1", key), "*/3 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})
	tm.AddTask(fmt.Sprintf("%s2", key), "*/3 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})
	select {}

}
