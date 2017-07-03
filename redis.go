package dbr

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"os"
	"time"
)

func NewRedis(url, password string, dbIndex, maxActive, maxIdle int) (p *Pool) {
	var dialFunc = func() (c redigo.Conn, err error) {
		if len(password) > 0 {
			c, err = redigo.Dial("tcp", url, redigo.DialPassword(password))
		} else {
			c, err = redigo.Dial("tcp", url)
		}

		if err != nil {
			fmt.Println("连接 Redis 服务器失败:", url, err)
			os.Exit(-1)
		}

		_, err = c.Do("SELECT", dbIndex)
		if err != nil {
			fmt.Println("Redis 执行 SELECT 指令失败:", dbIndex, err)
			c.Close()
			os.Exit(-1)
		}

		return c, err
	}

	p = &Pool{}
	var pool = redigo.NewPool(dialFunc, maxIdle)
	pool.MaxActive = maxActive
	pool.IdleTimeout = 180 * time.Second
	pool.Wait = true
	p.p = pool

	return p
}

////////////////////////////////////////////////////////////////////////////////
type Pool struct {
	p *redigo.Pool
}

func (this *Pool) GetSession() *Session {
	var c = this.p.Get()
	return NewSession(c)
}

func (this *Pool) Release(s *Session) {
	s.c.Close()
}

////////////////////////////////////////////////////////////////////////////////
func NewSession(c Conn) *Session {
	if c == nil {
		return nil
	}
	return &Session{c: c}
}

type Session struct {
	c Conn
}

func (this *Session) Conn() redigo.Conn {
	return this.c
}

func (this *Session) Close() {
	if this.c != nil {
		this.c.Close()
	}
}

func (this *Session) Do(commandName string, args ...interface{}) (*Result) {
	return result(this.c.Do(commandName, args...))
}

func (this *Session) Send(commandName string, args ...interface{}) (*Result) {
	var err = this.c.Send(commandName, args...)
	var r = result(nil, err)
	return r
}

////////////////////////////////////////////////////////////////////////////////
func (this *Session) Transaction(callback func(conn Conn)) (*Result) {
	var c = this.c
	c.Send("MULTI")
	callback(c)
	return result(c.Do("EXEC"))
}

func (this *Session) Pipeline(callback func(conn Conn)) (err error) {
	var c = this.c
	callback(c)
	return c.Flush()
}

////////////////////////////////////////////////////////////////////////////////
type Conn interface {
	redigo.Conn
}

////////////////////////////////////////////////////////////////////////////////
const k_REDIS_KEY = "redis_conn"

type Context interface {
	Set(key string, value interface{})

	MustGet(key string) interface{}
}

func FromContext(ctx Context) *Session {
	return ctx.MustGet(k_REDIS_KEY).(*Session)
}

func ToContext(ctx Context, s *Session) {
	ctx.Set(k_REDIS_KEY, s)
}
