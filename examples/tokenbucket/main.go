package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/tokenbucket"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)

	var limiter = tokenbucket.New(rClient, "tb")

	for i := 0; i < 11; i++ {
		fmt.Println(limiter.Allow(context.Background()))
	}
}
