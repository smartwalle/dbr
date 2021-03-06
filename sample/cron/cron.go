package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
	"time"
)

func main() {
	var pool = dbr.NewRedis("127.0.0.1:6379", 10, 2, redis.DialDatabase(15))
	//var pool2 = dbr.NewRedis("127.0.0.1:6380", 10, 2, redis.DialDatabase(15))
	//var pool3 = dbr.NewRedis("127.0.0.1:6381", 10, 2, redis.DialDatabase(15))
	var rs = dbr.NewRedSync(pool)

	var c = dbr.NewCron("test", pool, dbr.WithCronRedSync(rs))

	for i :=0; i<100; i++ {
		c.Add(fmt.Sprintf("task-%d", i), "*/1 * * * *", func(name string) {
			fmt.Println(name, time.Now())
		})
	}

	//c.Add(fmt.Sprintf("%s2", key), "*/5 * * * *", func(name string) {
	//	fmt.Println(name, time.Now())
	//})

	//c.UpdateNextTime(fmt.Sprintf("%s2", key), time.Now().Add(time.Minute*2))

	select {}

}
