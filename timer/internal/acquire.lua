local scheduledKey = KEYS[1]
local messagePrefix = KEYS[2]

local limit = tonumber(ARGV[1])

local nowParts = redis.call('TIME')
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)

local ids = redis.call('ZRANGEBYSCORE', scheduledKey, '-inf', now, 'LIMIT', 0, limit)
local result = {now, 0}

for _, id in ipairs(ids) do
	if redis.call('ZREM', scheduledKey, id) == 1 then
		local messageKey = messagePrefix .. id
		local body = redis.call('HGET', messageKey, 'body') or ''
		redis.call('DEL', messageKey)
		table.insert(result, id)
		table.insert(result, body)
	end
end

local nextValues = redis.call('ZRANGE', scheduledKey, 0, 0, 'WITHSCORES')
if nextValues[2] then
	result[2] = tonumber(nextValues[2]) or 0
end

return result
