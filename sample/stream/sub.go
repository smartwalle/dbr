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
			var sList, err = sSess.XREAD(0, 0, "email", "$").Streams()
			fmt.Println(err)
			for _, s := range sList {
				fmt.Println("consumer-1", s.Key, s.Id)
				for _, f := range s.Fields {
					fmt.Println("--", f.Field, f.Value)
				}
			}
		}
	}()

	select {}
}
