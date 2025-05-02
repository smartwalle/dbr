package internal

import (
	_ "embed"
	"github.com/redis/go-redis/v9"
)

// 消息结构(hash)
// id -- 消息业务id
// uuid -- 消息唯一id
// deliver_at -- 投递时间（毫秒）
// queue -- 队列名称
// body -- 消息内容
// retry_remain -- 剩余重试次数
// retry_delay -- 重试延迟时间（秒）
// created_at -- 消息创建时间
// consumer -- 当前消费者

// 待消费队列(sorted set) - member: MessageKey(id)，score: 消费时间
// 就绪队列(list) - element: MessageKey(uuid)
// 处理中队列(sorted set) - member: MessageKey(uuid), score: 确认处理成功超时时间
// 消费失败队列(list) - element: MessageKey(uuid)

//go:embed schedule_message.lua
var scheduleMessageScript string

// ScheduleMessageScript 添加消息
var ScheduleMessageScript = redis.NewScript(scheduleMessageScript)

//go:embed remove_message.lua
var removeMessageScript string

// RemoveMessageScript 删除消息
var RemoveMessageScript = redis.NewScript(removeMessageScript)

//go:embed message.lua
var messageScript string

// MessageScript 消息信息
var MessageScript = redis.NewScript(messageScript)

//go:embed pending_to_ready.lua
var pendingToReadyScript string

// PendingToReadyScript 将消息从[待消费队列]转移到[就绪队列]
var PendingToReadyScript = redis.NewScript(pendingToReadyScript)

//go:embed ready_to_running.lua
var readyToRunningScript string

// ReadyToRunningScript 将消息从[就绪队列]转移到[处理中队列]
var ReadyToRunningScript = redis.NewScript(readyToRunningScript)

//go:embed running_to_pending.lua
var runningToPendingScript string

// RunningToPendingScript 将[处理中队列]中已经消费超时的消息转移到[待重试队列]
var RunningToPendingScript = redis.NewScript(runningToPendingScript)

//go:embed ack.lua
var ackScript string

// AckScript 消费成功
var AckScript = redis.NewScript(ackScript)

//go:embed nack.lua
var nackScript string

// NackScript 消费失败
var NackScript = redis.NewScript(nackScript)

//go:embed init_consumer.lua
var initConsumerScript string

// InitConsumerScript 初始消费者
var InitConsumerScript = redis.NewScript(initConsumerScript)

//go:embed keep_consumer.lua
var keeyConsumerScript string

// KeepConsumerScript 上报消费者
var KeepConsumerScript = redis.NewScript(keeyConsumerScript)

//go:embed remove_consumer.lua
var removeConsumerScript string

// RemoveConsumerScript 删除消费者
var RemoveConsumerScript = redis.NewScript(removeConsumerScript)

//go:embed clear_consumer.lua
var clearConsumerScript string

// ClearConsumerScript 清理超时的消费者
var ClearConsumerScript = redis.NewScript(clearConsumerScript)
