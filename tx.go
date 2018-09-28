package dbr

// var p = dbr.NewRedis("127.0.0.1:6379", "", 1, 30, 1)
//
// var rSess = p.GetSession()
// defer rSess.Close()
//
// rSess.BeginTx()
// rSess.SET("k1", "v1")
// rSess.SET("k2", "v2")
// rSess.GET("k1")
// rSess.Commit()

func (this *Session) Watch(keys ...string) *Result {
	var ks []interface{}
	for _, k := range keys {
		ks = append(ks, k)
	}
	return this.Do("WATCH", ks...)
}

func (this *Session) UnWatch() *Result {
	return this.Do("UNWATCH")
}

func (this *Session) BeginTx() *Result {
	return this.Send("MULTI")
}

func (this *Session) Rollback() *Result {
	return this.Send("DISCARD")
}

func (this *Session) Commit() *Result {
	return this.Do("EXEC")
}
