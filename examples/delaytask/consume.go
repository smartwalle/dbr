package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/delaytask"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)
	var task = delaytask.New(
		rClient,
		"mail",
		delaytask.WithFetchInterval(time.Millisecond*500),
		delaytask.WithHandler(func(ctx context.Context, message *delaytask.Message) bool {
			fmt.Println(time.Now().UnixMilli(), "Consume End", message.ID(), message.UUID(), message.Body())
			return true
		}),
	)

	defer task.Stop(context.Background())
	go func() {
		fmt.Println("消费者准备就绪")
		if err := task.Start(context.Background()); err != nil {
			fmt.Println("Start Error:", err)
		}
		fmt.Println("消费结束")
	}()

	var sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	select {
	case <-sig:
	}
}
