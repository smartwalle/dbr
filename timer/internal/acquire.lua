local scheduledKey = KEYS[1]
local messagePrefix = KEYS[2]

local limit = tonumber(ARGV[1])

local nowParts = redis.call('TIME')
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)

local ids = redis.call('ZRANGEBYSCORE', scheduledKey, '-inf', now, 'LIMIT', 0, limit)
local result = {}

for _, id in ipairs(ids) do
	if redis.call('ZREM', scheduledKey, id) == 1 then
		local messageKey = messagePrefix .. id
		local body = redis.call('HGET', messageKey, 'body') or ''
		redis.call('DEL', messageKey)
		table.insert(result, id)
		table.insert(result, body)
	end
end

return result
