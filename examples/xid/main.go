package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/xid"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "192.168.2.229:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)

	var idGen, err = xid.New(rClient)
	if err != nil {
		fmt.Println(err)
		return
	}

	var now = time.Now()
	for i := 0; i < 100; i++ {
		fmt.Println(idGen.Next(context.Background()))
	}
	fmt.Println(time.Now().Sub(now))
}
