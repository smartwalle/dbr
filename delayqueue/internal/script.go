package internal

import (
	_ "embed"
	"github.com/redis/go-redis/v9"
)

// 消息结构(hash)
// id -- 消息业务id
// uuid -- 消息唯一id
// dt -- 消费时间（投递时间）
// qn -- 队列名称
// bd -- 消息内容
// rc -- 剩余重试次数
// ct -- 消息创建时间
// cid -- 当前消费者id

// 待消费队列(sorted set) - member: MessageKey(id)，score: 消费时间
// 就绪队列(list) - element: MessageKey(uuid)
// 处理中队列(sorted set) - member: MessageKey(uuid), score: 确认处理成功超时时间
// 待重试队列(sorted set) - member: MessageKey(uuid), score: 下次消费时间

//go:embed schedule.lua
var scheduleScript string

// ScheduleScript 添加消息
var ScheduleScript = redis.NewScript(scheduleScript)

//go:embed remove.lua
var removeScript string

// RemoveScript 删除消息
var RemoveScript = redis.NewScript(removeScript)

//go:embed pending_to_ready.lua
var pendingToReadyScript string

// PendingToReadyScript 将消息从[待消费队列]转移到[就绪队列]
var PendingToReadyScript = redis.NewScript(pendingToReadyScript)

//go:embed ready_to_active.lua
var readyToActiveScript string

// ReadyToActiveScript 将消息从[就绪队列]转移到[处理中队列]
var ReadyToActiveScript = redis.NewScript(readyToActiveScript)

//go:embed active_to_retry.lua
var activeToRetryScript string

// ActiveToRetryScript 将[处理中队列]中已经消费超时的消息转移到[待重试队列]
var ActiveToRetryScript = redis.NewScript(activeToRetryScript)

//go:embed ack.lua
var ackScript string

// AckScript 消费成功
var AckScript = redis.NewScript(ackScript)

//go:embed nack.lua
var nackScript string

// NackScript 消费失败
var NackScript = redis.NewScript(nackScript)

//go:embed retry_to_active.lua
var retryToAciveScript string

// RetryToAciveScript 将消息从[待重试队列]转移到[处理中队列]
var RetryToAciveScript = redis.NewScript(retryToAciveScript)

//go:embed clear_consumer.lua
var clearConsumerScript string

// ClearConsumerScript 清理超时的消费者
var ClearConsumerScript = redis.NewScript(clearConsumerScript)
