package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
)

func main() {
	var pool = dbr.NewRedis("192.168.1.99:6379", 10, 2, redis.DialDatabase(15))

	go func() {
		var sSess = pool.GetSession()

		for {
			var sList = sSess.XREAD(0, 0, "test_key", "0").MustStreams()
			for _, s := range sList {
				fmt.Println("consumer-1", s.Key, s.Id)
				for f, v := range s.Fields {
					fmt.Println("--", f, v)
				}

				sSess.XDEL("test_key", s.Id)
			}
		}
	}()
	select {}
}
