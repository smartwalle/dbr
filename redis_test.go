package dbr

import (
	//"testing"
	//"fmt"
)

var pool *Pool

func getPool() *Pool {
	if pool == nil {
		pool = NewRedis("127.0.0.1:6379", "", 1, 30, 10)
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