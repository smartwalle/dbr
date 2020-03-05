package main

import (
	"fmt"
	"github.com/smartwalle/dbr"
	"sync"
)

func main() {
	var p = dbr.NewRedis("127.0.0.1:6379", 15, 30)

	var wg = &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		var rSess = p.GetSession()
		defer rSess.Close()
		rSess.Sub(func(channel, data string) error {
			fmt.Println("---1---", channel, data)
			return nil
		}, "my-sub1", "my-sub2")
		wg.Done()
	}()

	go func() {
		var rSess = p.GetSession()
		defer rSess.Close()
		rSess.Sub(func(channel, data string) error {
			fmt.Println("---2---", channel, data)
			return nil
		}, "my-sub1", "my-sub2")
		wg.Done()
	}()

	wg.Wait()
	fmt.Println("end...")

	//var psc = redis.PubSubConn{Conn: rSess.Conn()}
	//psc.Subscribe("hello")
	//defer psc.Close()
	//
	//for {
	//	switch v := psc.Receive().(type) {
	//	case redis.Message:
	//		fmt.Println("msg", v.Channel, string(v.Data))
	//	case redis.Subscription:
	//		fmt.Println("sub", v.Channel, v.Kind, v.Count)
	//	case error:
	//		fmt.Println("sorry", v)
	//	}
	//}
}
