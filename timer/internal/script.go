package internal

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed schedule.lua
var scheduleScriptSource string

var ScheduleScript = redis.NewScript(scheduleScriptSource)

//go:embed cancel.lua
var cancelScriptSource string

var CancelScript = redis.NewScript(cancelScriptSource)

//go:embed acquire.lua
var acquireScriptSource string

var AcquireScript = redis.NewScript(acquireScriptSource)
