package timer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func testRedisClient(t *testing.T) redis.UniversalClient {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: "192.168.2.229:6379",
		DB:   1,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skip("Redis unavailable:", err)
	}
	return client
}

func testQueue() string {
	return "test-" + uuid.NewString()
}

func TestScheduleUpdateCancel(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	queue := testQueue()
	task := New(client, queue)

	id := "task-1"
	if err := task.Schedule(ctx, id, WithBody("hello"), WithDelay(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if err := task.Schedule(ctx, id, WithBody("hello again"), WithDelay(2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	ok, err := task.Cancel(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected cancel to remove scheduled task")
	}

	ok, err = task.Cancel(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected second cancel to report missing task")
	}
}

func TestNextPollInterval(t *testing.T) {
	task := New(nil, "test", WithPollInterval(time.Second))

	tests := []struct {
		name   string
		result acquireResult
		want   time.Duration
	}{
		{
			name:   "empty queue uses poll interval",
			result: acquireResult{now: 1000, nextRunAt: 0},
			want:   time.Second,
		},
		{
			name:   "future message uses shorter interval",
			result: acquireResult{now: 1000, nextRunAt: 1100},
			want:   100 * time.Millisecond,
		},
		{
			name:   "far future message uses poll interval",
			result: acquireResult{now: 1000, nextRunAt: 3000},
			want:   time.Second,
		},
		{
			name:   "near message uses minimum interval",
			result: acquireResult{now: 1000, nextRunAt: 1001},
			want:   minPollInterval,
		},
		{
			name:   "due message uses minimum interval",
			result: acquireResult{now: 1000, nextRunAt: 1000},
			want:   minPollInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := task.nextPollInterval(tt.result); got != tt.want {
				t.Fatalf("nextPollInterval() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNextPollIntervalHonorsSmallPollInterval(t *testing.T) {
	task := New(nil, "test", WithPollInterval(5*time.Millisecond))
	result := acquireResult{now: 1000, nextRunAt: 1001}

	if got := task.nextPollInterval(result); got != 5*time.Millisecond {
		t.Fatalf("nextPollInterval() = %s, want %s", got, 5*time.Millisecond)
	}
}

func TestStartHonorsMaxInFlight(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	queue := testQueue()

	const total = 6
	const maxInFlight = 2

	var running int64
	var peak int64
	var handled int64
	var wg sync.WaitGroup
	wg.Add(total)

	task := New(
		client,
		queue,
		WithMaxInFlight(maxInFlight),
		WithPollInterval(10*time.Millisecond),
	)
	task.OnMessage(func(ctx context.Context, message *Message) {
		current := atomic.AddInt64(&running, 1)
		for {
			old := atomic.LoadInt64(&peak)
			if current <= old || atomic.CompareAndSwapInt64(&peak, old, current) {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&running, -1)
		atomic.AddInt64(&handled, 1)
		wg.Done()
	})

	for i := 0; i < total; i++ {
		if err := task.Schedule(ctx, fmt.Sprintf("task-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Start(startCtx)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for tasks")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Start to return")
	}

	if got := atomic.LoadInt64(&handled); got != total {
		t.Fatalf("handled = %d, want %d", got, total)
	}
	if got := atomic.LoadInt64(&peak); got > maxInFlight {
		t.Fatalf("peak in flight = %d, want <= %d", got, maxInFlight)
	}
}

func TestOnMessageTriggered(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	queue := testQueue()

	done := make(chan struct{})
	task := New(
		client,
		queue,
		WithPollInterval(10*time.Millisecond),
	)
	task.OnMessage(func(ctx context.Context, message *Message) {
		if message.Id() != "manual-task" {
			t.Errorf("message id = %q, want manual-task", message.Id())
		}
		close(done)
	})

	if err := task.Schedule(ctx, "manual-task", WithBody("payload")); err != nil {
		t.Fatal(err)
	}

	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Start(startCtx)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Start to return")
	}
}

func TestOnMessagePanicUsesPanicHandler(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	queue := testQueue()

	panicCh := make(chan any, 1)
	errorCh := make(chan error, 1)

	task := New(client, queue, WithPollInterval(10*time.Millisecond))
	task.OnMessage(func(ctx context.Context, message *Message) {
		panic("boom")
	})
	task.OnError(func(ctx context.Context, err error) {
		errorCh <- err
	})
	task.OnPanic(func(ctx context.Context, value any) {
		panicCh <- value
	})

	if err := task.Schedule(ctx, "panic-task"); err != nil {
		t.Fatal(err)
	}

	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Start(startCtx)
	}()

	select {
	case value := <-panicCh:
		if value != "boom" {
			t.Fatalf("panic value = %v, want boom", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for panic handler")
	}

	select {
	case err := <-errorCh:
		t.Fatalf("unexpected error handler call: %v", err)
	default:
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Start to return")
	}
}

func TestStartState(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	queue := testQueue()

	task := New(client, queue, WithPollInterval(10*time.Millisecond))
	task.OnMessage(func(ctx context.Context, message *Message) {})

	startCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Start(startCtx)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := task.Start(ctx); !errors.Is(err, ErrTimerRunning) {
		t.Fatalf("second Start error = %v, want %v", err, ErrTimerRunning)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Start to return")
	}

	if err := task.Start(ctx); !errors.Is(err, ErrTimerClosed) {
		t.Fatalf("Start after closed error = %v, want %v", err, ErrTimerClosed)
	}
}

func TestMultipleInstancesTriggerEachMessageOnce(t *testing.T) {
	client := testRedisClient(t)
	ctx := context.Background()
	queue := testQueue()

	const total = 20

	var mu sync.Mutex
	seen := make(map[string]int, total)
	done := make(chan struct{})
	closed := false

	onMessage := func(ctx context.Context, message *Message) {
		mu.Lock()
		seen[message.Id()]++
		if len(seen) == total && !closed {
			closed = true
			close(done)
		}
		mu.Unlock()
	}

	task1 := New(client, queue, WithMaxInFlight(4), WithPollInterval(10*time.Millisecond))
	task1.OnMessage(onMessage)

	task2 := New(client, queue, WithMaxInFlight(4), WithPollInterval(10*time.Millisecond))
	task2.OnMessage(onMessage)

	for i := 0; i < total; i++ {
		if err := task1.Schedule(ctx, fmt.Sprintf("task-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		errCh <- task1.Start(startCtx)
	}()
	go func() {
		errCh <- task2.Start(startCtx)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for messages")
	}

	cancel()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for Start to return")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s triggered %d times, want 1", id, n)
		}
	}
}
