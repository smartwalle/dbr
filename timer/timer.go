package timer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/timer/internal"
)

// MessageHandler 是定时消息触发后的处理函数。
//
// 消息在调用该函数前已经从 Redis 中删除，Timer 不关心处理结果，
// 也不会因为处理失败再次触发同一条消息。
type MessageHandler func(ctx context.Context, message *Message)

// ErrorHandler 处理 Redis 命令、启动循环等内部错误。
// MessageHandler 中的 panic 不会进入这里，会进入 PanicHandler。
type ErrorHandler func(ctx context.Context, err error)

// PanicHandler 处理 MessageHandler 中产生的 panic。
type PanicHandler func(ctx context.Context, value any)

// State 表示 Timer 的本地生命周期状态。
type State int32

const (
	// StateIdle 表示 Timer 尚未启动。
	StateIdle State = 0
	// StateRunning 表示 Timer 正在运行。
	StateRunning State = 1
	// StateClosed 表示 Timer 已关闭；关闭后不能再次启动。
	StateClosed State = 2
)

// Option 用于配置 Timer。
type Option func(timer *Timer)

// WithMaxInFlight 设置单个 Timer 实例内同时执行的消息处理数量。
// 多个实例同时运行时，总并发量为各实例 maxInFlight 之和。
func WithMaxInFlight(maximum int) Option {
	return func(timer *Timer) {
		if maximum > 0 {
			timer.maxInFlight = maximum
		}
	}
}

// WithPollInterval 设置轮询 Redis 的间隔。
func WithPollInterval(interval time.Duration) Option {
	return func(timer *Timer) {
		if interval > 0 {
			timer.pollInterval = interval
		}
	}
}

// WithCommandTimeout 设置单次 Redis 命令的超时时间。
func WithCommandTimeout(timeout time.Duration) Option {
	return func(timer *Timer) {
		if timeout > 0 {
			timer.commandTimeout = timeout
		}
	}
}

var (
	ErrInvalidMessage  = errors.New("invalid message")
	ErrHandlerRequired = errors.New("handler required")
	ErrTimerRunning    = errors.New("timer is running")
	ErrTimerClosed     = errors.New("timer closed")
)

// Timer 是一个基于 Redis 的简单分布式定时器。
//
// Timer 只保证同一条消息在 Redis 层面被一个实例领取一次。
// 消息被领取后会立即从 Redis 删除，之后的业务处理结果不影响 Timer 状态。
type Timer struct {
	client redis.UniversalClient
	queue  string
	state  State

	scheduledKey     string
	messagePrefixKey string

	messageHandler atomic.Value
	errorHandler   atomic.Value
	panicHandler   atomic.Value

	maxInFlight    int
	pollInterval   time.Duration
	commandTimeout time.Duration

	mu           sync.Mutex
	rootCancel   context.CancelFunc
	rootFinished chan struct{}
	inFlight     atomic.Int64
	wg           sync.WaitGroup
}

// New 创建一个 Timer。
// queue 用于隔离不同业务的 Redis key。
func New(client redis.UniversalClient, queue string, opts ...Option) *Timer {
	timer := &Timer{
		client:           client,
		queue:            queue,
		scheduledKey:     internal.ScheduledKey(queue),
		messagePrefixKey: internal.MessagePrefixKey(queue),
		maxInFlight:      1,
		pollInterval:     time.Second,
		commandTimeout:   5 * time.Second,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(timer)
		}
	}
	timer.normalize()
	return timer
}

func (timer *Timer) normalize() {
	if timer.maxInFlight <= 0 {
		timer.maxInFlight = 1
	}
	if timer.pollInterval <= 0 {
		timer.pollInterval = time.Second
	}
	if timer.commandTimeout <= 0 {
		timer.commandTimeout = 5 * time.Second
	}
}

// OnMessage 注册定时消息触发后的处理函数。
// Start 前必须注册该处理函数。
func (timer *Timer) OnMessage(handler MessageHandler) {
	if handler == nil {
		return
	}
	timer.messageHandler.Store(handler)
}

// OnError 注册内部错误处理函数。
func (timer *Timer) OnError(handler ErrorHandler) {
	if handler == nil {
		return
	}
	timer.errorHandler.Store(handler)
}

// OnPanic 注册 MessageHandler panic 处理函数。
func (timer *Timer) OnPanic(handler PanicHandler) {
	if handler == nil {
		return
	}
	timer.panicHandler.Store(handler)
}

// Schedule 创建或更新消息；同一个 id 在触发前再次调用会更新触发时间和内容。
func (timer *Timer) Schedule(ctx context.Context, id string, opts ...TaskOption) error {
	if id == "" {
		return ErrInvalidMessage
	}

	taskOpts := taskOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&taskOpts)
		}
	}
	keys := []string{
		timer.scheduledKey,
		internal.MessageKey(timer.queue, id),
	}
	args := []any{
		id,
		taskOpts.body,
		taskOpts.delay.Milliseconds(),
	}

	ctx, cancel := timer.commandContext(ctx)
	defer cancel()

	return internal.ScheduleScript.Run(ctx, timer.client, keys, args...).Err()
}

// Cancel 取消尚未触发的消息。
// 返回 true 表示消息存在且已取消；返回 false 表示消息不存在或已经被触发。
func (timer *Timer) Cancel(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, ErrInvalidMessage
	}

	keys := []string{
		timer.scheduledKey,
		internal.MessageKey(timer.queue, id),
	}
	args := []any{id}

	ctx, cancel := timer.commandContext(ctx)
	defer cancel()

	value, err := internal.CancelScript.Run(ctx, timer.client, keys, args...).Int()
	if err != nil {
		return false, err
	}
	return value == 1, nil
}

// Start 启动定时器消费循环。
// 该方法会阻塞直到 ctx 被取消、Stop 被调用或启动阶段返回错误。
func (timer *Timer) Start(ctx context.Context) error {
	if timer.messageHandler.Load() == nil {
		return ErrHandlerRequired
	}

	if err := timer.client.Ping(ctx).Err(); err != nil {
		return err
	}

	timer.mu.Lock()
	switch timer.state {
	case StateRunning:
		timer.mu.Unlock()
		return ErrTimerRunning
	case StateClosed:
		timer.mu.Unlock()
		return ErrTimerClosed
	}

	ctx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})

	timer.state = StateRunning
	timer.rootCancel = cancel
	timer.rootFinished = finished
	timer.mu.Unlock()

	defer func() {
		cancel()
		timer.wg.Wait()

		timer.mu.Lock()
		timer.state = StateClosed
		timer.rootCancel = nil
		timer.rootFinished = nil
		timer.mu.Unlock()

		close(finished)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		available := timer.maxInFlight - int(timer.inFlight.Load())
		if available <= 0 {
			if !timer.sleep(ctx) {
				return nil
			}
			continue
		}

		messages, err := timer.acquire(ctx, available)
		if err != nil {
			timer.handleError(ctx, err)
			if !timer.sleep(ctx) {
				return nil
			}
			continue
		}

		for _, message := range messages {
			timer.inFlight.Add(1)
			timer.wg.Add(1)
			go timer.handleMessage(ctx, message)
		}

		if len(messages) == 0 || len(messages) < available {
			if !timer.sleep(ctx) {
				return nil
			}
		}
	}
}

// Stop 停止定时器，并等待已经触发的 MessageHandler 返回。
func (timer *Timer) Stop(ctx context.Context) error {
	timer.mu.Lock()
	var cancel = timer.rootCancel
	var done = timer.rootFinished
	switch timer.state {
	case StateClosed:
		timer.mu.Unlock()
		return nil
	default:
		timer.state = StateClosed
	}
	timer.mu.Unlock()

	if cancel == nil || done == nil {
		return nil
	}

	cancel()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquire 原子领取已到期消息；领取成功的消息会立即从 Redis 删除。
func (timer *Timer) acquire(ctx context.Context, limit int) ([]*Message, error) {
	if limit <= 0 {
		return nil, nil
	}

	keys := []string{
		timer.scheduledKey,
		timer.messagePrefixKey,
	}
	args := []any{limit}

	ctx, cancel := timer.commandContext(ctx)
	defer cancel()

	raw, err := internal.AcquireScript.Run(ctx, timer.client, keys, args...).Slice()
	if err != nil {
		return nil, err
	}

	messages := make([]*Message, 0, len(raw)/2)
	for i := 0; i+1 < len(raw); i += 2 {
		messages = append(messages, &Message{
			id:    stringValue(raw[i]),
			queue: timer.queue,
			body:  stringValue(raw[i+1]),
		})
	}
	return messages, nil
}

func (timer *Timer) handleMessage(ctx context.Context, message *Message) {
	defer func() {
		timer.inFlight.Add(-1)
		timer.wg.Done()

		if x := recover(); x != nil {
			timer.handlePanic(ctx, x)
		}
	}()

	handler := timer.messageHandler.Load()
	if handler == nil {
		return
	}

	handler.(MessageHandler)(ctx, message)
}

func (timer *Timer) handleError(ctx context.Context, err error) {
	if err == nil {
		return
	}
	handler := timer.errorHandler.Load()
	if handler == nil {
		return
	}

	defer func() {
		_ = recover()
	}()
	handler.(ErrorHandler)(ctx, err)
}

func (timer *Timer) handlePanic(ctx context.Context, value any) {
	handler := timer.panicHandler.Load()
	if handler == nil {
		return
	}

	defer func() {
		_ = recover()
	}()
	handler.(PanicHandler)(ctx, value)
}

func (timer *Timer) commandContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timer.commandTimeout)
}

func (timer *Timer) sleep(ctx context.Context) bool {
	t := time.NewTimer(timer.pollInterval)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
