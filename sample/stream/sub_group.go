package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
)

func main() {
	var pool = dbr.NewRedis("192.168.1.99:6379", 10, 2, redis.DialDatabase(15))

	//var ss= pool.GetSession()
	//ss.XGROUPDESTROY("email", "g1")
	//ss.XGROUPDESTROY("email", "g2")
	//ss.Close()

	go func() {
		var sSess = pool.GetSession()
		defer sSess.Close()

		fmt.Println(sSess.XGROUPCREATE("email", "g1", "$", "MKSTREAM"))

		for {
			var sList, err = sSess.XREADGROUP("g1", "c11", 0, 0, "email", ">").Streams()
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, s := range sList {
				fmt.Println("consumer-1-1", s.Key, s.Id)
				for f, v := range s.Fields {
					fmt.Println("--", f, v)
				}
				sSess.XACK("email", "g1", s.Id)
			}
		}
	}()

	go func() {
		var sSess = pool.GetSession()
		defer sSess.Close()

		fmt.Println(sSess.XGROUPCREATE("email", "g1", "$", "MKSTREAM"))

		for {
			var sList, err = sSess.XREADGROUP("g1", "c12", 0, 0, "email", ">").Streams()
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, s := range sList {
				fmt.Println("consumer-1-2", s.Key, s.Id)
				for f, v := range s.Fields {
					fmt.Println("--", f, v)
				}
				sSess.XACK("email", "g1", s.Id)
			}
		}
	}()

	go func() {
		var sSess = pool.GetSession()
		defer sSess.Close()

		fmt.Println(sSess.XGROUPCREATE("email", "g2", "$", "MKSTREAM"))

		for {
			var sList, err = sSess.XREADGROUP("g2", "c21", 0, 0, "email", ">").Streams()
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, s := range sList {
				fmt.Println("consumer-2-1", s.Key, s.Id)
				for f, v := range s.Fields {
					fmt.Println("--", f, v)
				}
				sSess.XACK("email", "g2", s.Id)
			}
		}
	}()

	select {}
}
