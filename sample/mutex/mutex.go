package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
	"time"
)

func main() {
	var pool = dbr.NewRedis("192.168.1.99:6379", 10, 2, redis.DialDatabase(15))
	var rs = dbr.NewRedSync(pool)

	for {
		var mu = rs.NewMutex("test_mutex")
		if err := mu.Lock(); err != nil {
			fmt.Println("加锁失败", err)
			continue
		}

		fmt.Println("加锁成功")

		time.Sleep(time.Second * 3)

		if mu.Unlock() == false {
			fmt.Println("解锁失败")
			continue
		}
	}

}
