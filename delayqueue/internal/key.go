package internal

import "fmt"

func QueueKey(qname string) string {
	return fmt.Sprintf("dbr:delayqueue:{%s}", qname)
}

// PendingKey 用于构建[待消费队列]名字
func PendingKey(qname string) string {
	return fmt.Sprintf("%s:pending", QueueKey(qname))
}

// ReadyKey 用于构建[就绪队列]名字
func ReadyKey(qname string) string {
	return fmt.Sprintf("%s:ready", QueueKey(qname))
}

// ActiveKey 用于构建[处理中队列]名字
func ActiveKey(qname string) string {
	return fmt.Sprintf("%s:active", QueueKey(qname))
}

// RetryKey 用于构建[待重试队列]名字
func RetryKey(qname string) string {
	return fmt.Sprintf("%s:retry", QueueKey(qname))
}

// ConsumerKey 用于构建[消费者队列]名字
func ConsumerKey(qname string) string {
	return fmt.Sprintf("%s:consumer", QueueKey(qname))
}

// MessageKeyPrefix 用于构建[消息]前缀名字
func MessageKeyPrefix(qname string) string {
	return fmt.Sprintf("%s:m:", QueueKey(qname))
}

// MessageKey 用于构建[消息]的名字
func MessageKey(qname, id string) string {
	return fmt.Sprintf("%s%s", MessageKeyPrefix(qname), id)
}
