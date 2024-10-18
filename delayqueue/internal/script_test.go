package internal_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/delayqueue/internal"
	"os"
	"testing"
	"time"
)

var redisClient redis.UniversalClient
var queue = "test"
var consumer = "test-consumer"

func TestMain(m *testing.M) {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)
	if _, err := rClient.Ping(context.Background()).Result(); err != nil {
		fmt.Println("redis ping error:", err)
		return
	}
	redisClient = rClient
	os.Exit(m.Run())
}

func Test_QueueKey(t *testing.T) {
	t.Log(internal.QueueKey(queue))
	t.Log(internal.PendingKey(queue))
	t.Log(internal.ReadyKey(queue))
	t.Log(internal.ActiveKey(queue))
	t.Log(internal.RetryKey(queue))
	t.Log(internal.MessageKey(queue, "11"))
	t.Log(internal.MessageKey(queue, "22"))
}

func Test_ScheduleScript(t *testing.T) {
	var id = "t1"

	var keys = []string{
		internal.PendingKey(queue),
		internal.MessageKey(queue, id),
	}
	var args = []interface{}{
		id,
		uuid.New().String(),
		time.Now().UnixMilli(),
		queue,
		"message body",
		2,
	}
	raw, err := internal.ScheduleScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_RemoveScript(t *testing.T) {
	var id = "t1"

	var keys = []string{
		internal.PendingKey(queue),
		internal.MessageKey(queue, id),
	}
	var args = []interface{}{
		id,
	}
	raw, err := internal.RemoveScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func reportConsumer(t *testing.T) {
	_, err := redisClient.ZAddNX(context.Background(), internal.ConsumerKey(queue), redis.Z{Member: consumer, Score: float64(time.Now().UnixMilli() + 60*1000)}).Result()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_PendingToReadyScript(t *testing.T) {
	reportConsumer(t)

	var keys = []string{
		internal.PendingKey(queue),
		internal.ReadyKey(queue),
		internal.MessagePrefixKey(queue),
	}
	var args = []interface{}{
		10,
	}
	raw, err := internal.PendingToReadyScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_ReadyToActiveScript(t *testing.T) {
	reportConsumer(t)

	var keys = []string{
		internal.ReadyKey(queue),
		internal.ActiveKey(queue),
		internal.ConsumerKey(queue),
	}
	var args = []interface{}{
		consumer,
	}
	raw, err := internal.ReadyToActiveScript.Run(context.Background(), redisClient, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_ActiveToRetryScript(t *testing.T) {
	reportConsumer(t)

	var keys = []string{
		internal.ActiveKey(queue),
		internal.RetryKey(queue),
		internal.ConsumerKey(queue),
	}
	var args = []interface{}{
		5,
	}
	raw, err := internal.ActiveToRetryScript.Run(context.Background(), redisClient, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_AckScript(t *testing.T) {
	var keys = []string{
		internal.ActiveKey(queue),
		internal.MessageKey(queue, "8adaf494-1c70-4017-93bc-5786a26ea6b0"),
	}
	raw, err := internal.AckScript.Run(context.Background(), redisClient, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_NackScript(t *testing.T) {
	var keys = []string{
		internal.ActiveKey(queue),
		internal.RetryKey(queue),
		internal.MessageKey(queue, "8adaf494-1c70-4017-93bc-5786a26ea6b0"),
	}
	var args = []interface{}{
		5,
	}
	raw, err := internal.NackScript.Run(context.Background(), redisClient, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_RetryToAciveScript(t *testing.T) {
	var keys = []string{
		internal.RetryKey(queue),
		internal.ActiveKey(queue),
		internal.ConsumerKey(queue),
	}
	var args = []interface{}{
		consumer,
	}
	raw, err := internal.RetryToAciveScript.Run(context.Background(), redisClient, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}
