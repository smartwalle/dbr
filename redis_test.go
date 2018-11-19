package dbr

import (
//"testing"
//"fmt"
)

type People struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

var pool *Pool

func getPool() *Pool {
	if pool == nil {
		pool = NewRedis("192.168.1.99:6379", "", 0, 30, 10)
	}
	return pool
}

func getSession() *Session {
	var s = getPool().GetSession()
	return s
}

//func TestRedis(t *testing.T) {
//
//	var c = s.GetSession()
//
//	var p1 = Plan{Title:"pt1", Text:"t1"}
//	c.HMSET("p1", &p1)
//
//	var p2 Plan
//
//	fmt.Println(c.HGETALL("p1", &p2))
//	fmt.Println(p2)
//}
//
//type Plan struct {
//	Title string `redis:"title"`
//	Text  string `redis:"text"`
//}
