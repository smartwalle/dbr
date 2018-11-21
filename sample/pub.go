package main

import (
	"github.com/smartwalle/dbr"
)

func main() {
	var p = dbr.NewRedis("127.0.0.1:6379", 15, 30)

	var rSess = p.GetSession()

	rSess.Send("MULTI")
	rSess.Send("PUBLISH", "hello", "1")
	rSess.Send("PUBLISH", "hello", "2")
	rSess.Send("PUBLISH", "hello", "3")
	rSess.Send("PUBLISH", "hello", "4")
	rSess.Do("EXEC")

	rSess.Close()
}
