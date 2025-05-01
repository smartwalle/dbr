package delaytask

import (
	"github.com/google/uuid"
	"time"
)

func NewUUID() string {
	return uuid.New().String()
}

// Message 消息结构 (hash)
type Message struct {
	id          string // 消息 id - id
	uuid        string // 消息 uuid - uuid
	queue       string // 队列名称 - queue
	body        string // 消息内容 - body
	retryRemain int    // 剩余重试次数 - retry_remain
	retryDelay  int    // 重试延迟时间（秒）- retry_delay
	deliverAt   int64  // 投递时间（毫秒） - deliver_at
}

func (m *Message) ID() string {
	return m.id
}

func (m *Message) UUID() string {
	return m.uuid
}

func (m *Message) Queue() string {
	return m.queue
}

func (m *Message) Body() string {
	return m.body
}

type MessageOption func(m *Message)

func WithDeliverAt(deliverAt time.Time) MessageOption {
	return func(m *Message) {
		if deliverAt.IsZero() {
			m.deliverAt = time.Now().UnixMilli()
		} else {
			m.deliverAt = deliverAt.UnixMilli()
		}
	}
}

func WithDeliverAfter(seconds int64) MessageOption {
	return func(m *Message) {
		if seconds < 0 {
			seconds = 0
		}
		m.deliverAt = time.Now().UnixMilli() + (seconds * 1000)
	}
}

func WithBody(body string) MessageOption {
	return func(m *Message) {
		m.body = body
	}
}

func WithMaxRetries(retries int) MessageOption {
	return func(m *Message) {
		m.retryRemain = retries
	}
}

func WithRetryDelay(seconds int) MessageOption {
	return func(m *Message) {
		if seconds <= 0 {
			m.retryDelay = 5
		}
		m.retryDelay = seconds
	}
}
