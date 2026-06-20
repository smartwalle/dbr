package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/timer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	defer stop()

	rClient := redis.NewClient(&redis.Options{
		Addr: "192.168.2.229:6379",
		DB:   1,
	})
	if err := rClient.Ping(ctx).Err(); err != nil {
		fmt.Println("redis ping error:", err)
		os.Exit(1)
	}

	task := timer.New(
		rClient,
		"timer-demo",
		timer.WithMaxInFlight(1),
		timer.WithPollInterval(200*time.Millisecond),
	)

	task.OnError(func(ctx context.Context, err error) {
		fmt.Println("error:", err)
	})

	task.OnPanic(func(ctx context.Context, value any) {
		fmt.Printf("panic: %v\n", value)
	})

	task.OnMessage(func(ctx context.Context, message *timer.Message) {
		fmt.Printf("consume: id=%s body=%s\n", message.Id(), message.Body())
	})

	fmt.Println("timer consumer started")
	if err := task.Start(ctx); err != nil {
		fmt.Println("start error:", err)
	}
}
