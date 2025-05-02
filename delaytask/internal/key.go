package internal

import "fmt"

func QueueKey(queue string) string {
	return fmt.Sprintf("dbr:delaytask:{%s}", queue)
}

// PendingKey 用于构建[待消费队列]名字
func PendingKey(queue string) string {
	return fmt.Sprintf("%s:pending", QueueKey(queue))
}

// ReadyKey 用于构建[就绪队列]名字
func ReadyKey(queue string) string {
	return fmt.Sprintf("%s:ready", QueueKey(queue))
}

// RunningKey 用于构建[处理中队列]名字
func RunningKey(queue string) string {
	return fmt.Sprintf("%s:running", QueueKey(queue))
}

// FailureKey 用于构建[消费失败队列]名字
func FailureKey(queue string) string {
	return fmt.Sprintf("%s:failure", QueueKey(queue))
}

// ConsumerKey 用于构建[消费者队列]名字
func ConsumerKey(queue string) string {
	return fmt.Sprintf("%s:consumer", QueueKey(queue))
}

// MessagePrefixKey 用于构建[消息]前缀名字
func MessagePrefixKey(queue string) string {
	return fmt.Sprintf("%s:message:", QueueKey(queue))
}

// MessageKey 用于构建[消息]的名字
func MessageKey(queue, id string) string {
	return fmt.Sprintf("%s:message:%s", QueueKey(queue), id)
	//return fmt.Sprintf("%s%s", MessagePrefixKey(qname), id)
}
