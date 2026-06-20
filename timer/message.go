package timer

import "time"

type Message struct {
	id    string
	queue string
	body  string
}

// Id 返回消息 id。
func (m *Message) Id() string {
	return m.id
}

// Queue 返回消息所属队列。
func (m *Message) Queue() string {
	return m.queue
}

// Body 返回消息内容。
func (m *Message) Body() string {
	return m.body
}

type taskOptions struct {
	body  string
	delay time.Duration
}

type TaskOption func(opts *taskOptions)

// WithBody 设置消息内容。
func WithBody(body string) TaskOption {
	return func(opts *taskOptions) {
		opts.body = body
	}
}

// WithDelay 设置消息延迟多久后触发。
func WithDelay(delay time.Duration) TaskOption {
	return func(opts *taskOptions) {
		if delay < 0 {
			delay = 0
		}
		opts.delay = delay
	}
}
