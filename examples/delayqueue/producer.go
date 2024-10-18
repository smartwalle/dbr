package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)

	var queue, err = delayqueue.NewDelayQueue(rClient, "mail")
	if err != nil {
		fmt.Println("NewDelayQueue Error", err)
		return
	}

	for i := 0; i < 10; i++ {
		fmt.Println(i, queue.Enqueue(context.Background(), fmt.Sprintf("%d", i), delayqueue.WithDeliverAfter(0), delayqueue.WithMaxRetry(1), delayqueue.WithBody(fmt.Sprintf("body-%d", i))))
	}
}
