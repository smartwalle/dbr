package internal

func QueueKey(queue string) string {
	return "dbr:timer:" + queue
}

func ScheduledKey(queue string) string {
	return QueueKey(queue) + ":scheduled"
}

func MessagePrefixKey(queue string) string {
	return QueueKey(queue) + ":message:"
}

func MessageKey(queue string, id string) string {
	return MessagePrefixKey(queue) + id
}
