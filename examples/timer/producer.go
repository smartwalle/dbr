package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/timer"
)

func main() {
	ctx := context.Background()

	rClient := redis.NewClient(&redis.Options{
		Addr: "192.168.2.229:6379",
		DB:   1,
	})
	if err := rClient.Ping(ctx).Err(); err != nil {
		fmt.Println("redis ping error:", err)
		os.Exit(1)
	}

	task := timer.New(rClient, "timer-demo")

	for i := 0; i < 1000; i++ {
		if err := task.Schedule(ctx, fmt.Sprintf("%d", i)); err != nil {
			fmt.Println("schedule error:", err)
			return
		}
	}
	fmt.Println("schedule:", 1000)

	//if err := task.Schedule(ctx, "immediate", timer.WithBody("run now")); err != nil {
	//	fmt.Println("schedule immediate error:", err)
	//	return
	//}
	//fmt.Println("schedule: immediate")
	//
	//if err := task.Schedule(ctx, "delayed", timer.WithDelay(2*time.Second), timer.WithBody("run after 2s")); err != nil {
	//	fmt.Println("schedule delayed error:", err)
	//	return
	//}
	//fmt.Println("schedule: delayed")
	//
	//if err := task.Schedule(ctx, "update", timer.WithDelay(10*time.Second), timer.WithBody("updated to 1s")); err != nil {
	//	fmt.Println("schedule update error:", err)
	//	return
	//}
	//if err := task.Schedule(ctx, "update", timer.WithDelay(time.Second), timer.WithBody("updated to 1s")); err != nil {
	//	fmt.Println("schedule update error:", err)
	//	return
	//}
	//fmt.Println("schedule update: update")
	//
	//if err := task.Schedule(ctx, "cancel", timer.WithDelay(3*time.Second), timer.WithBody("will be canceled")); err != nil {
	//	fmt.Println("schedule cancel error:", err)
	//	return
	//}
	//ok, err := task.Cancel(ctx, "cancel")
	//if err != nil {
	//	fmt.Println("cancel error:", err)
	//	return
	//}
	//fmt.Println("cancel:", ok)

	//if err := task.Schedule(ctx, "another", timer.WithDelay(time.Second), timer.WithBody("another timer")); err != nil {
	//	fmt.Println("schedule another error:", err)
	//	return
	//}
	//fmt.Println("schedule: another")
}
