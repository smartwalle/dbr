package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/delaytask"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)

	var task = delaytask.New(rClient, "mail")

	for i := 0; i < 1000; i++ {
		fmt.Println(i, task.Enqueue(context.Background(), fmt.Sprintf("%d", i), delaytask.WithDeliverAfter(10), delaytask.WithMaxRetries(3), delaytask.WithRetryDelay(5), delaytask.WithBody(fmt.Sprintf("body-%d", i))))
	}
}
