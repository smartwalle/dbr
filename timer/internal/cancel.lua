local scheduledKey = KEYS[1]
local messageKey = KEYS[2]

local id = ARGV[1]

local removed = redis.call('ZREM', scheduledKey, id)
if removed == 1 then
	redis.call('DEL', messageKey)
	return 1
end

return 0
