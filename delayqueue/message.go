package delayqueue

import (
	"github.com/google/uuid"
	"time"
)

func NewUUID() string {
	return uuid.New().String()
}

// Message 消息结构 (hash)
type Message struct {
	id        string // 消息 id - id
	uuid      string // 消息 uuid - uuid
	queue     string // 队列名称 - qn
	body      string // 消息内容 - bd
	retry     int    // 剩余重试次数 - rc
	deliverAt int64  // 消费时间（投递时间）- dt
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

func (m *Message) DeliverAt() int64 {
	return m.deliverAt
}

type MessageOption func(m *Message)

func WithDeliverAt(deliverAt time.Time) MessageOption {
	return func(m *Message) {
		if deliverAt.IsZero() {
			m.deliverAt = 0
		} else {
			m.deliverAt = deliverAt.UnixMilli()
		}
	}
}

func WithDeliverAfter(seconds int64) MessageOption {
	return func(m *Message) {
		m.deliverAt = time.Now().UnixMilli() + (seconds * 1000)
	}
}

func WithBody(body string) MessageOption {
	return func(m *Message) {
		m.body = body
	}
}

func WithMaxRetry(maxRetry int) MessageOption {
	return func(m *Message) {
		m.retry = maxRetry
	}
}
