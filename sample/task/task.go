package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
	"time"
)

func main() {
	var pool = dbr.NewRedis("127.0.0.1:6379", 10, 2, redis.DialDatabase(15))
	var rs = dbr.NewRedSync(pool)

	var key = "x3"

	var tm = dbr.NewTaskManager(key, pool, dbr.WithTaskRedSync(rs))

	tm.AddTask(key, "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})
	select {}

}
