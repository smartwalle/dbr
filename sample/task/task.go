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

	var tm = dbr.NewTaskManager("xx", pool, rs)

	tm.AddTask("x1", "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})

	tm.AddTask("x2", "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})

	tm.AddTask("x3", "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})

	tm.AddTask("x4", "*/1 * * * *", func(name string) {
		fmt.Println(name, time.Now())
	})

	select {}

}
