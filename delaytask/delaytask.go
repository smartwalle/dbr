package delaytask

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dbr/delaytask/internal"
	"golang.org/x/sync/errgroup"
	"sync/atomic"
	"time"
)

type Handler func(ctx context.Context, message *Message) bool

type Option func(delayTask *DelayTask)

func WithHandler(handler Handler) Option {
	return func(delayTask *DelayTask) {
		delayTask.handler = handler
	}
}

func WithFetchLimit(limit int) Option {
	return func(delayTask *DelayTask) {
		if limit < 0 {
			limit = 1
		}
		delayTask.fetchLimit = limit
	}
}

func WithFetchInterval(interval time.Duration) Option {
	return func(delayTask *DelayTask) {
		if interval <= 0 {
			interval = time.Second
		}
		delayTask.fetchInterval = interval
	}
}

func WithHeartbeatInterval(interval time.Duration) Option {
	return func(delayTask *DelayTask) {
		if interval <= 10 {
			interval = time.Second * 30
		}
		delayTask.fetchInterval = interval
	}
}

func WithMaxInFlight(maximum int) Option {
	return func(delayTask *DelayTask) {
		if maximum <= 0 {
			maximum = 1
		}
		delayTask.maxInFlight = maximum
	}
}

type DelayTask struct {
	cancel func()

	client     redis.UniversalClient
	uuid       string
	queue      string
	handler    Handler
	inShutdown atomic.Bool

	pengingKey       string
	readyKey         string
	runningKey       string
	consumerKey      string
	messagePrefixKey string

	fetchLimit        int           // 单次将消息从[待消费队列]转移到[就绪队列]的最大数量
	fetchInterval     time.Duration // 查询可处理消息间隔时间，当[就绪队列]中没有消息，等待多久进行下一次查询
	heartbeatInterval time.Duration // 消费者心跳间隔时间，消息者存活时间为 heartbeatInterval * 2
	maxInFlight       int           // 同时处理消息数量，即处理消息 Goroutine 数量
}

var (
	ErrInvalidMessage = errors.New("invalid message")
	ErrConsumerExists = errors.New("consumer exists")
	ErrConsumerClosed = errors.New("consumer closed")
)

func New(client redis.UniversalClient, queue string, opts ...Option) *DelayTask {
	var delayTask = &DelayTask{}
	delayTask.client = client
	delayTask.uuid = NewUUID()
	delayTask.queue = queue

	delayTask.pengingKey = internal.PendingKey(queue)
	delayTask.readyKey = internal.ReadyKey(queue)
	delayTask.runningKey = internal.RunningKey(queue)
	delayTask.consumerKey = internal.ConsumerKey(queue)
	delayTask.messagePrefixKey = internal.MessagePrefixKey(queue)

	delayTask.fetchLimit = 1000
	delayTask.fetchInterval = time.Second          // 默认1秒
	delayTask.heartbeatInterval = time.Second * 30 // 默认30秒
	delayTask.maxInFlight = 1                      // 默认1
	for _, opt := range opts {
		if opt != nil {
			opt(delayTask)
		}
	}
	return delayTask
}

func (delayTask *DelayTask) UUID() string {
	return delayTask.uuid
}

func (delayTask *DelayTask) Schedule(ctx context.Context, id string, opts ...MessageOption) error {
	if id == "" {
		return ErrInvalidMessage
	}
	var m = &Message{}
	m.id = id
	m.uuid = NewUUID()
	m.queue = delayTask.queue
	m.retryDelay = 5 // 默认 5 秒后重试
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	var keys = []string{
		delayTask.pengingKey,
		internal.MessageKey(delayTask.queue, m.id),
	}
	var args = []interface{}{
		m.id,
		m.uuid,
		m.deliverAt,
		m.queue,
		m.body,
		m.retryRemain,
		m.retryDelay,
	}
	_, err := internal.ScheduleMessageScript.Run(ctx, delayTask.client, keys, args...).Result()
	if err != nil {
		return err
	}
	return nil
}

func (delayTask *DelayTask) Remove(ctx context.Context, id string) error {
	if id == "" {
		return ErrInvalidMessage
	}

	var keys = []string{
		delayTask.pengingKey,
		internal.MessageKey(delayTask.queue, id),
	}
	var args = []interface{}{
		id,
	}
	_, err := internal.RemoveMessageScript.Run(ctx, delayTask.client, keys, args...).Result()
	if err != nil {
		return err
	}
	return nil
}

func (delayTask *DelayTask) pendingToReady(ctx context.Context) error {
	var keys = []string{
		delayTask.pengingKey,
		delayTask.readyKey,
		delayTask.messagePrefixKey,
	}
	var args = []interface{}{
		delayTask.fetchLimit,
	}
	_, err := internal.PendingToReadyScript.Run(ctx, delayTask.client, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (delayTask *DelayTask) readyToRunningScript(ctx context.Context) (string, error) {
	var keys = []string{
		delayTask.readyKey,
		delayTask.runningKey,
		delayTask.consumerKey,
	}
	var args = []interface{}{
		delayTask.uuid,
	}
	raw, err := internal.ReadyToRunningScript.Run(ctx, delayTask.client, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	uuid, _ := raw.(string)
	return uuid, nil
}

func (delayTask *DelayTask) runningToPendingScript(ctx context.Context) error {
	var keys = []string{
		delayTask.runningKey,
		delayTask.pengingKey,
		delayTask.consumerKey,
	}
	_, err := internal.RunningToPendingScript.Run(ctx, delayTask.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (delayTask *DelayTask) ack(ctx context.Context, uuid string) error {
	var keys = []string{
		delayTask.runningKey,
		internal.MessageKey(delayTask.queue, uuid),
	}
	_, err := internal.AckScript.Run(ctx, delayTask.client, keys).Result()
	if err != nil {
		return err
	}
	return nil
}

func (delayTask *DelayTask) nack(ctx context.Context, uuid string) error {
	var keys = []string{
		delayTask.runningKey,
		delayTask.pengingKey,
		internal.MessageKey(delayTask.queue, uuid),
	}
	_, err := internal.NackScript.Run(ctx, delayTask.client, keys).Result()
	if err != nil {
		return err
	}
	return nil
}

func (delayTask *DelayTask) initConsumer(ctx context.Context) error {
	var keys = []string{
		delayTask.consumerKey,
	}
	var args = []interface{}{
		delayTask.uuid,
		time.Now().UnixMilli() + delayTask.heartbeatInterval.Milliseconds()*2,
	}

	value, err := internal.InitConsumerScript.Run(ctx, delayTask.client, keys, args...).Int()
	if err != nil {
		return err
	}
	if value != 1 {
		return ErrConsumerExists
	}
	return nil
}

func (delayTask *DelayTask) keepConsumer(ctx context.Context) error {
	var keys = []string{
		delayTask.consumerKey,
	}
	var args = []interface{}{
		delayTask.uuid,
		time.Now().UnixMilli() + delayTask.heartbeatInterval.Milliseconds()*2,
	}

	value, err := internal.KeepConsumerScript.Run(ctx, delayTask.client, keys, args...).Int()
	if err != nil {
		return err
	}
	if value != 1 {
		return ErrConsumerClosed
	}
	return nil
}

func (delayTask *DelayTask) removeConsumer(ctx context.Context) error {
	var keys = []string{
		delayTask.consumerKey,
	}
	var args = []interface{}{
		delayTask.uuid,
	}

	_, err := internal.RemoveConsumerScript.Run(ctx, delayTask.client, keys, args...).Int()
	if err != nil {
		return err
	}
	return nil
}

func (delayTask *DelayTask) clearConsumer(ctx context.Context) error {
	var keys = []string{
		delayTask.consumerKey,
	}
	_, err := internal.ClearConsumerScript.Run(ctx, delayTask.client, keys).Result()
	if err != nil {
		return err
	}
	return nil
}

func (delayTask *DelayTask) consumeMessage(ctx context.Context, uuid string) error {
	defer func() {
		var r = recover()
		if r != nil {
			delayTask.nack(ctx, uuid)
		}
	}()
	if uuid == "" {
		return nil
	}

	var keys = []string{
		internal.MessageKey(delayTask.queue, uuid),
	}

	raw, err := internal.MessageScript.Run(ctx, delayTask.client, keys).StringSlice()
	if err != nil {
		return err
	}

	var message = &Message{}
	message.id = raw[0]
	message.uuid = raw[1]
	message.queue = raw[2]
	message.body = raw[3]

	if delayTask.handler != nil {
		if ok := delayTask.handler(ctx, message); ok {
			return delayTask.ack(ctx, uuid)
		}
	}
	return delayTask.nack(ctx, uuid)
}

func (delayTask *DelayTask) consume(ctx context.Context) (err error) {
	if err = delayTask.pendingToReady(ctx); err != nil {
		return err
	}

	var uuid = ""

	// 消费消息
	for {
		uuid, err = delayTask.readyToRunningScript(ctx)
		if err != nil {
			return err
		}
		if uuid == "" {
			break
		}
		if err = delayTask.consumeMessage(ctx, uuid); err != nil {
			return err
		}
	}

	return nil
}

func (delayTask *DelayTask) Start(ctx context.Context) (err error) {
	if delayTask.inShutdown.Load() {
		return ErrConsumerClosed
	}

	if err = delayTask.client.Ping(ctx).Err(); err != nil {
		return err
	}

	ctx, delayTask.cancel = context.WithCancel(ctx)

	// 初始消费者
	if err = delayTask.initConsumer(ctx); err != nil {
		return err
	}

	group, ctx := errgroup.WithContext(ctx)

	// 定时上报消费者
	group.Go(func() error {
		var ticker = time.NewTicker(delayTask.heartbeatInterval)
		for {
			select {
			case <-ctx.Done():
				return delayTask.Stop(context.WithoutCancel(ctx))
			case <-ticker.C:
				// 上报消费者存活状态
				if nErr := delayTask.keepConsumer(ctx); nErr != nil {
					return nErr
				}

				// 清理超时的消费者
				if nErr := delayTask.clearConsumer(ctx); nErr != nil {
					return nErr
				}

				// 处理消费超时的消息
				if nErr := delayTask.runningToPendingScript(ctx); nErr != nil {
					return nErr
				}
			}
		}
	})

	// 消费消息
	for i := 0; i < delayTask.maxInFlight; i++ {
		group.Go(func() error {
			var ticker = time.NewTicker(delayTask.fetchInterval)
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					if nErr := delayTask.consume(ctx); nErr != nil {
						return nErr
					}
				}
			}
		})
	}

	if err = group.Wait(); err != nil {
		return err
	}

	return nil
}

func (delayTask *DelayTask) Stop(ctx context.Context) (err error) {
	if !delayTask.inShutdown.CompareAndSwap(false, true) {
		return nil
	}

	if delayTask.cancel != nil {
		delayTask.cancel()
	}
	if err = delayTask.removeConsumer(ctx); err != nil {
		return err
	}
	return nil
}
