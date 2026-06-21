// Package xid 提供基于 Redis 的 Snowflake 风格 uint64 ID 生成器。
//
// 生成器使用 Redis TIME 作为时间源，并通过 Lua 脚本原子保留同一 worker 下的
// 时间戳和序列号区间。worker 由 Redis 租约自动分配，多个实例使用同一个 Key 时
// 会竞争不同 worker；生成器运行期间会自动续约 worker，Close 会主动释放租约。
package xid

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	defaultKey              = "dbr:xid"
	defaultWorkerBits       = uint8(10)
	defaultSequenceBits     = uint8(12)
	defaultStateLease       = 24 * time.Hour
	defaultWorkerLease      = 30 * time.Second
	defaultMaxClockBackward = time.Second
	defaultMaxFutureDrift   = 1 * time.Second
	defaultRetryDelay       = 10 * time.Millisecond
	defaultMaxBatchSize     = 200
)

var (
	// ErrInvalidConfig 表示生成器配置不合法。
	ErrInvalidConfig = errors.New("invalid config")

	// ErrClockBackward 表示 Redis TIME 发生超过容忍范围的回拨，或当前时间早于 epoch。
	ErrClockBackward = errors.New("redis clock moved backward")

	// ErrTimeOverflow 表示当前时间戳已无法放入配置出的时间戳位宽。
	ErrTimeOverflow = errors.New("timestamp exceeds configured bit width")

	// ErrNoWorker 表示配置位宽下的 worker 都已被占用。
	ErrNoWorker = errors.New("no worker available")

	// ErrWorkerLeaseLost 表示当前实例不再持有 worker 租约。
	ErrWorkerLeaseLost = errors.New("worker lease lost")
)

// Options 控制 ID 的生成和解析方式。
//
// 默认布局接近常见 Snowflake 结构：
// 42 位时间戳、10 位 worker 和 12 位序列号。时间戳位数由无符号 64 位 ID
// 空间扣除 worker 和序列号位数后得到。
type Options struct {
	// Key 用于生成 Redis 状态 key 和 worker 租约 key。同一个 Redis 库中修改位宽
	// 或 epoch 时应使用不同 key，避免旧状态按新布局解析。
	Key string

	// Epoch 会在组装 ID 前从 Redis 服务端时间中扣除。epoch 越晚，可表示的结束时间
	// 越晚，但不能晚于 Redis 当前时间。
	Epoch time.Time

	// WorkerBits 是 worker 段位宽。可用 worker 数为 2^WorkerBits。
	WorkerBits uint8
	// SequenceBits 是同一毫秒内序列号位宽。每个 worker 每毫秒最多生成
	// 2^SequenceBits 个 ID。
	SequenceBits uint8

	// StateLease 是每次分配 ID 后设置给 Redis 状态的租约时间。状态 key 保存最近
	// 分配到的 ts/seq，用于防止同一 worker 的序列回退。
	StateLease time.Duration

	// WorkerLease 是自动分配 worker 后对应 Redis 租约的过期时间。生成器会在后台
	// 自动续约，Close 会主动释放。
	WorkerLease time.Duration

	// MaxClockBackward 是允许 Redis TIME 回拨的最大时长。小幅回拨会由逻辑时钟吸收，
	// 超过该值会返回 ErrClockBackward。
	MaxClockBackward time.Duration

	// RetryDelay 是逻辑时间临时领先 Redis 服务端时间时的重试间隔。
	RetryDelay time.Duration

	// MaxBatchSize 限制一次 NextBatch 最多可保留的 ID 数量，避免单次请求把逻辑
	// 时间推进过远。
	MaxBatchSize int
}

// Option 修改 Generator 配置。
type Option func(*Options)

// WithKey 设置用于存储生成器状态的 Redis key。
func WithKey(key string) Option {
	return func(opts *Options) {
		opts.Key = strings.TrimSpace(key)
	}
}

// WithEpoch 设置生成 ID 使用的自定义起始时间。
func WithEpoch(epoch time.Time) Option {
	return func(opts *Options) {
		opts.Epoch = epoch
	}
}

// WithBits 设置 worker 和序列号的位宽。
func WithBits(workerBits, sequenceBits uint8) Option {
	return func(opts *Options) {
		opts.WorkerBits = workerBits
		opts.SequenceBits = sequenceBits
	}
}

// WithStateLease 设置空闲 Redis 状态保留多久。
func WithStateLease(ttl time.Duration) Option {
	return func(opts *Options) {
		opts.StateLease = ttl
	}
}

// WithWorkerLease 设置自动分配 worker 的租约过期时间。
func WithWorkerLease(ttl time.Duration) Option {
	return func(opts *Options) {
		opts.WorkerLease = ttl
	}
}

// WithMaxClockBackward 设置允许 Redis 时钟回拨的最大时长。
func WithMaxClockBackward(duration time.Duration) Option {
	return func(opts *Options) {
		opts.MaxClockBackward = duration
	}
}

// WithRetryDelay 设置逻辑时间临时领先 Redis 服务端时间时的重试间隔。
func WithRetryDelay(delay time.Duration) Option {
	return func(opts *Options) {
		opts.RetryDelay = delay
	}
}

// WithMaxBatchSize 设置 NextBatch 单次最多可保留的 ID 数量。
func WithMaxBatchSize(n int) Option {
	return func(opts *Options) {
		opts.MaxBatchSize = n
	}
}

// Generator 使用 Redis TIME 和 Lua 脚本原子保留序列号，生成 Snowflake 风格 ID。
//
// Generator 会在后台自动续约 worker 租约。使用完成后应调用 Close 释放 worker。
type Generator struct {
	client redis.UniversalClient
	opts   Options
	key    string

	worker         uint64
	workerKey      string
	workerToken    string
	mu             sync.Mutex
	workerDeadline time.Time
	cancel         context.CancelFunc
	done           chan struct{}
	maxFutureDrift time.Duration

	stateKey      string
	workerShift   uint8
	timestampBits uint8
	sequenceMask  uint64
	timestampMask uint64
}

func defaultOptions() Options {
	return Options{
		Key:              defaultKey,
		Epoch:            time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		WorkerBits:       defaultWorkerBits,
		SequenceBits:     defaultSequenceBits,
		StateLease:       defaultStateLease,
		WorkerLease:      defaultWorkerLease,
		MaxClockBackward: defaultMaxClockBackward,
		RetryDelay:       defaultRetryDelay,
		MaxBatchSize:     defaultMaxBatchSize,
	}
}

// New 创建 Generator，并通过 Redis 自动分配 worker。
//
// New 会访问 Redis 抢占 worker 租约。当前构造函数不接收 context，因此生产环境应
// 在 redis client 上配置合理的 DialTimeout、ReadTimeout 和 WriteTimeout。
func New(client redis.UniversalClient, options ...Option) (*Generator, error) {
	opts := defaultOptions()
	for _, opt := range options {
		if opt != nil {
			opt(&opts)
		}
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	// uint64 最高位可用，时间戳位数由 worker 和 sequence 位宽反推。
	timestampBits := uint8(64 - int(opts.WorkerBits) - int(opts.SequenceBits))
	worker, workerKey, workerToken, err := acquireWorker(context.Background(), client, opts)
	if err != nil {
		return nil, err
	}

	g := &Generator{
		client:         client,
		opts:           opts,
		key:            opts.Key,
		worker:         worker,
		workerKey:      workerKey,
		workerToken:    workerToken,
		workerDeadline: time.Now().Add(opts.WorkerLease),
		maxFutureDrift: defaultMaxFutureDrift,
		stateKey:       buildStateKey(opts.Key, worker),
		workerShift:    opts.SequenceBits,
		timestampBits:  timestampBits,
		sequenceMask:   bitMask(opts.SequenceBits),
		timestampMask:  bitMask(timestampBits),
	}
	g.start()
	return g, nil
}

func validateOptions(opts Options) error {
	if opts.Key == "" {
		return fmt.Errorf("%w: key is empty", ErrInvalidConfig)
	}
	if opts.Epoch.IsZero() {
		return fmt.Errorf("%w: epoch is zero", ErrInvalidConfig)
	}
	if opts.WorkerBits == 0 {
		return fmt.Errorf("%w: worker bits must be positive", ErrInvalidConfig)
	}
	if opts.SequenceBits == 0 {
		return fmt.Errorf("%w: sequence bits must be positive", ErrInvalidConfig)
	}
	if int(opts.WorkerBits)+int(opts.SequenceBits) >= 64 {
		return fmt.Errorf("%w: worker bits plus sequence bits must be less than 64", ErrInvalidConfig)
	}
	if opts.StateLease < time.Second {
		return fmt.Errorf("%w: state lease must be at least one second", ErrInvalidConfig)
	}
	if opts.WorkerLease < time.Second {
		return fmt.Errorf("%w: worker lease ttl must be at least one second", ErrInvalidConfig)
	}
	if opts.MaxClockBackward < 0 {
		return fmt.Errorf("%w: max clock backward cannot be negative", ErrInvalidConfig)
	}
	if opts.RetryDelay <= 0 {
		return fmt.Errorf("%w: retry delay must be positive", ErrInvalidConfig)
	}
	if opts.MaxBatchSize <= 0 {
		return fmt.Errorf("%w: max batch size must be positive", ErrInvalidConfig)
	}
	return nil
}

// Next 返回一个全局唯一 ID。
func (g *Generator) Next(ctx context.Context) (uint64, error) {
	ids, err := g.NextBatch(ctx, 1)
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// NextBatch 原子保留 n 个 ID，并按升序返回。
//
// 批量申请只执行一次 Redis Lua 脚本，适合高吞吐场景。若同一 worker 的逻辑时间
// 暂时领先 Redis 时间过多，方法会按 RetryDelay 等待重试，直到成功或 ctx 取消。
func (g *Generator) NextBatch(ctx context.Context, n int) ([]uint64, error) {
	if n <= 0 {
		return nil, fmt.Errorf("%w: batch size must be positive", ErrInvalidConfig)
	}
	if n > g.opts.MaxBatchSize {
		return nil, fmt.Errorf("%w: batch size %d exceeds max %d", ErrInvalidConfig, n, g.opts.MaxBatchSize)
	}
	if int64(n) > g.maxIDsPerReserve() {
		return nil, fmt.Errorf("batch size %d cannot fit within max future drift %s", n, g.maxFutureDrift)
	}
	if err := g.ensureWorkerLease(ctx); err != nil {
		return nil, err
	}

	for {
		ids, retry, err := g.reserve(ctx, n)
		if err != nil {
			return nil, err
		}
		if !retry {
			return ids, nil
		}
		// Redis 返回 retry 表示逻辑时间领先过多，等待真实时间追上。
		if err = sleep(ctx, g.opts.RetryDelay); err != nil {
			return nil, err
		}
	}
}

func (g *Generator) reserve(ctx context.Context, n int) ([]uint64, bool, error) {
	// Lua 脚本在 Redis 内原子推进 ts/seq，并返回本次批量保留的起止位置。
	result, err := reserveScript.Run(ctx, g.client, []string{g.stateKey},
		int64(g.opts.StateLease/time.Millisecond),
		int64(g.opts.MaxClockBackward/time.Millisecond),
		int64(g.maxFutureDrift/time.Millisecond),
		n,
		g.sequenceMask,
	).Slice()
	if err != nil {
		return nil, false, err
	}
	if len(result) != 6 {
		return nil, false, fmt.Errorf("unexpected redis response: %v", result)
	}

	code, err := toInt64(result[0])
	if err != nil {
		return nil, false, err
	}
	redisNow, err := toInt64(result[1])
	if err != nil {
		return nil, false, err
	}
	startTs, err := toInt64(result[2])
	if err != nil {
		return nil, false, err
	}
	startSeq, err := toInt64(result[3])
	if err != nil {
		return nil, false, err
	}
	finalTs, err := toInt64(result[4])
	if err != nil {
		return nil, false, err
	}
	finalSeq, err := toInt64(result[5])
	if err != nil {
		return nil, false, err
	}

	switch code {
	case 0:
	case 1:
		// Redis TIME 明显回拨，不能继续生成，否则可能破坏趋势和唯一性。
		return nil, false, fmt.Errorf("%w: redis_now=%d last_timestamp=%d", ErrClockBackward, redisNow, startTs)
	case 2:
		// 逻辑时间已领先 Redis 时间超过内部阈值，由调用方等待后重试。
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("unexpected redis status code %d", code)
	}

	if finalSeq < 0 || uint64(finalSeq) > g.sequenceMask {
		return nil, false, fmt.Errorf("invalid sequence returned by redis: %d", finalSeq)
	}

	startElapsed := startTs - g.opts.Epoch.UnixMilli()
	finalElapsed := finalTs - g.opts.Epoch.UnixMilli()
	if startElapsed < 0 {
		return nil, false, fmt.Errorf("%w: redis time is before epoch", ErrClockBackward)
	}
	if uint64(finalElapsed) > g.timestampMask {
		return nil, false, fmt.Errorf("%w: elapsed milliseconds %d exceeds %d bits", ErrTimeOverflow, finalElapsed, g.timestampBits)
	}

	ids := make([]uint64, 0, n)
	ts := uint64(startElapsed)
	seq := uint64(startSeq)
	for len(ids) < n {
		id := g.compose(ts, seq)
		ids = append(ids, id)
		seq++
		if seq > g.sequenceMask {
			// 跨过当前毫秒的序列号上限后，推进到下一逻辑毫秒。
			seq = 0
			ts++
		}
	}
	return ids, false, nil
}

func (g *Generator) compose(elapsedMs, seq uint64) uint64 {
	id := (elapsedMs << (g.opts.WorkerBits + g.opts.SequenceBits)) |
		(g.worker << g.workerShift) |
		seq
	return id
}

func (g *Generator) maxIDsPerReserve() int64 {
	perMs := int64(g.sequenceMask + 1)
	ms := int64(g.maxFutureDrift/time.Millisecond) + 1
	return perMs * ms
}

// Worker 返回当前生成器自动分配到的 worker。
func (g *Generator) Worker() uint64 {
	return g.worker
}

// Close 停止后台续约并释放当前生成器持有的 worker 租约。
func (g *Generator) Close(ctx context.Context) error {
	g.mu.Lock()
	cancel := g.cancel
	done := g.done
	g.mu.Unlock()

	if cancel != nil {
		// 先停止后台续约，避免释放 worker 时续约脚本并发执行。
		cancel()
		if done != nil {
			<-done
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.done == done {
		// 只清理当前这轮后台任务，避免并发场景误清理新任务状态。
		g.cancel = nil
		g.done = nil
	}
	if g.workerToken == "" {
		return nil
	}
	if err := releaseWorkerScript.Run(ctx, g.client, []string{g.workerKey}, g.workerToken).Err(); err != nil {
		return err
	}
	g.clearWorker()
	return nil
}

func bitMask(bits uint8) uint64 {
	return (uint64(1) << bits) - 1
}

func buildStateKey(key string, worker uint64) string {
	return key + ":state:" + strconv.FormatUint(worker, 10)
}

func buildWorkerKey(key string, worker uint64) string {
	return key + ":worker:" + strconv.FormatUint(worker, 10)
}

func acquireWorker(ctx context.Context, client redis.UniversalClient, opts Options) (uint64, string, string, error) {
	token := uuid.NewString()
	maxWorker := bitMask(opts.WorkerBits)

	for worker := uint64(0); worker <= maxWorker; worker++ {
		key := buildWorkerKey(opts.Key, worker)
		// SET NX + 过期时间用于抢占 worker 租约，token 用于后续续约和释放校验。
		ok, err := client.SetNX(ctx, key, token, opts.WorkerLease).Result()
		if err != nil {
			return 0, "", "", err
		}
		if ok {
			return worker, key, token, nil
		}
	}

	return 0, "", "", ErrNoWorker
}

// ensureWorkerLease 在生成 ID 前确认当前实例仍持有 worker 租约。
func (g *Generator) ensureWorkerLease(ctx context.Context) error {
	g.mu.Lock()
	if g.workerToken == "" {
		g.mu.Unlock()
		return ErrWorkerLeaseLost
	}
	if time.Until(g.workerDeadline) > g.opts.WorkerLease/3 {
		// 距离租约过期还比较久时，直接复用后台续约结果。
		g.mu.Unlock()
		return nil
	}
	g.mu.Unlock()

	return g.renewWorkerLease(ctx)
}

// renewWorkerLease 续约 worker 租约。
func (g *Generator) renewWorkerLease(ctx context.Context) error {
	// 方法内部加锁，调用方不需要持有 g.mu。
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.workerToken == "" {
		return ErrWorkerLeaseLost
	}
	ok, err := renewWorkerScript.Run(ctx, g.client, []string{g.workerKey},
		g.workerToken,
		int64(g.opts.WorkerLease/time.Millisecond),
	).Bool()
	if err != nil {
		return err
	}
	if !ok {
		// token 不匹配说明 worker 已被释放、过期或被其它实例持有。
		g.clearWorker()
		return ErrWorkerLeaseLost
	}

	g.workerDeadline = time.Now().Add(g.opts.WorkerLease)
	return nil
}

// clearWorker 清理本地 worker 状态。
func (g *Generator) clearWorker() {
	// 调用方已持有 g.mu。
	g.workerToken = ""
	g.workerDeadline = time.Time{}
}

// start 启动组件后台任务，目前用于自动续约 worker 租约。
func (g *Generator) start() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	g.mu.Lock()
	g.cancel = cancel
	g.done = done
	g.mu.Unlock()

	go func() {
		defer close(done)

		ticker := time.NewTicker(g.workerLeaseRenewDelay())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := g.renewWorkerLease(ctx); err != nil {
					if errors.Is(err, ErrWorkerLeaseLost) {
						// 租约已丢失，后台续约没有继续运行的意义。
						return
					}
				}
			}
		}
	}()
}

func (g *Generator) workerLeaseRenewDelay() time.Duration {
	delay := g.opts.WorkerLease / 3
	if delay < g.opts.RetryDelay {
		return g.opts.RetryDelay
	}
	return delay
}

func toInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected redis integer type %T", value)
	}
}

func sleep(ctx context.Context, delay time.Duration) error {
	var timer = time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var reserveScript = redis.NewScript(`
	local ttl_ms = tonumber(ARGV[1])
	local max_backward_ms = tonumber(ARGV[2])
	local max_future_ms = tonumber(ARGV[3])
	local batch = tonumber(ARGV[4])
	local sequence_mask = tonumber(ARGV[5])
	local per_ms = sequence_mask + 1

	local redis_time = redis.call("TIME")
	local redis_now = redis_time[1] * 1000 + math.floor(redis_time[2] / 1000)
	local last_ts = tonumber(redis.call("HGET", KEYS[1], "ts")) or -1
	local last_seq = tonumber(redis.call("HGET", KEYS[1], "seq")) or -1
	local now = redis_now

	if now < last_ts then
		if last_ts - now > max_backward_ms then
			return {1, redis_now, last_ts, last_seq, last_ts, last_seq}
		end
		now = last_ts
	end

	local start_ts = now
	local start_seq = 0

	if now == last_ts then
		start_seq = last_seq + 1
		if start_seq > sequence_mask then
			start_ts = last_ts + 1
			start_seq = 0
		end
	end

	local final_offset = start_seq + batch - 1
	local final_ts = start_ts + math.floor(final_offset / per_ms)
	local final_seq = final_offset % per_ms

	if final_ts - redis_now > max_future_ms then
		return {2, redis_now, start_ts, start_seq, final_ts, final_seq}
	end

	redis.call("HSET", KEYS[1], "ts", final_ts, "seq", final_seq)
	redis.call("PEXPIRE", KEYS[1], ttl_ms)

	return {0, redis_now, start_ts, start_seq, final_ts, final_seq}
`)

var renewWorkerScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	end
	return 0
`)

var releaseWorkerScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	end
	return 0
`)
