package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/delayqueue"
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
	var queue, err = delayqueue.New(rClient, "mail", delayqueue.WithFetchInterval(time.Millisecond*100))
	if err != nil {
		fmt.Println("NewDelayQueue Error", err)
		return
	}
	err = queue.StartConsume(func(m *delayqueue.Message) bool {
		fmt.Println(time.Now().UnixMilli(), "Consume", m.ID(), m.UUID(), m.Body(), m.DeliverAt())
		return true
	})
	if err != nil {
		fmt.Println("Consume Error", err)
		return
	}

	fmt.Println("消费者准备就绪")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
	fmt.Println("Close", queue.StopConsume())
}
