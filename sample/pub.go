package main

import (
	"fmt"
	"github.com/smartwalle/dbr"
)

func main() {
	var p = dbr.NewRedis("127.0.0.1:6379", 15, 30)

	var rSess = p.GetSession()

	fmt.Println(rSess.Pub("my-sub1", "haha1").Error)
	fmt.Println(rSess.Pub("my-sub2", "hehe1").Error)

	fmt.Println(rSess.Pub("my-sub1", "haha2").Error)
	fmt.Println(rSess.Pub("my-sub2", "hehe2").Error)

	//rSess.Send("MULTI")
	//rSess.Send("PUBLISH", "hello", "1")
	//rSess.Send("PUBLISH", "hello", "2")
	//rSess.Send("PUBLISH", "hello", "3")
	//rSess.Send("PUBLISH", "hello", "4")
	//rSess.Do("EXEC")

	rSess.Close()
}
