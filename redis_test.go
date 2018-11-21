package dbr

type People struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

var pool *Pool

func getPool() *Pool {
	if pool == nil {
		pool = NewRedis("192.168.1.99:6379", 10, 2)
	}
	return pool
}

func getSession() *Session {
	var s = getPool().GetSession()
	return s
}
