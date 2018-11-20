package dbr

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
