-- KEYS[1] - 消息结构

return redis.call('HMGET', KEYS[1], 'id', 'uuid', 'queue', 'body')