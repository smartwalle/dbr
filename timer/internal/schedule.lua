local scheduledKey = KEYS[1]
local messageKey = KEYS[2]

local id = ARGV[1]
local body = ARGV[2]
local delay = tonumber(ARGV[3]) or 0

local nowParts = redis.call('TIME')
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
if delay < 0 then
	delay = 0
end
local triggerAt = now + delay

if redis.call('EXISTS', messageKey) == 0 then
	redis.call('HSET', messageKey, 'created_at', now)
end

redis.call('HSET', messageKey,
	'id', id,
	'body', body,
	'run_at', triggerAt,
	'updated_at', now
)
redis.call('ZADD', scheduledKey, triggerAt, id)

return 1
