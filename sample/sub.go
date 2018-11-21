package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/smartwalle/dbr"
)

func main() {
	var p = dbr.NewRedis("127.0.0.1:6379", 15, 30)

	var rSess = p.GetSession()

	var psc = redis.PubSubConn{Conn: rSess.Conn()}
	psc.Subscribe("hello")
	defer psc.Close()

	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			fmt.Println("msg", v.Channel, string(v.Data))
		case redis.Subscription:
			fmt.Println("sub", v.Channel, v.Kind, v.Count)
		case error:
			fmt.Println("sorry", v)
		}
	}
}
