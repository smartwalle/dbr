package dbr

import (
	"errors"
	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"
	"time"
)

var (
	InvalidConnErr = errors.New("invalid connection")
)

func NewRedis(addr, password string, dbIndex, maxActive, maxIdle int) (p *Pool) {
	var dialFunc = func() (c redis.Conn, err error) {
		if len(password) > 0 {
			c, err = redis.Dial("tcp", addr, redis.DialPassword(password))
		} else {
			c, err = redis.Dial("tcp", addr)
		}

		if err != nil {
			return nil, err
		}

		_, err = c.Do("SELECT", dbIndex)
		if err != nil {
			c.Close()
			return nil, err
		}

		return c, err
	}

	p = &Pool{}
	var pool = &redis.Pool{}
	pool.MaxIdle = maxIdle
	pool.MaxActive = maxActive
	pool.Wait = true
	pool.IdleTimeout = 180 * time.Second
	pool.Dial = dialFunc
	p.Pool = pool

	return p
}

func NewRedisWithSentinel(addrs []string, masterName, password string, dbIndex, maxActive, maxIdle int) (p *Pool) {
	var s = &sentinel.Sentinel{
		Addrs:      addrs,
		MasterName: masterName,
		Dial: func(addr string) (redis.Conn, error) {
			timeout := 500 * time.Millisecond
			c, err := redis.Dial("tcp", addr, redis.DialReadTimeout(timeout), redis.DialWriteTimeout(timeout), redis.DialConnectTimeout(timeout))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}

	var dialFunc = func() (c redis.Conn, err error) {
		addr, err := s.MasterAddr()

		if len(password) > 0 {
			c, err = redis.Dial("tcp", addr, redis.DialPassword(password))
		} else {
			c, err = redis.Dial("tcp", addr)
		}

		if err != nil {
			return nil, err
		}

		_, err = c.Do("SELECT", dbIndex)
		if err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}

	var testOnBorrow = func(c redis.Conn, t time.Time) error {
		if !sentinel.TestRole(c, "master") {
			return errors.New("role check failed")
		} else {
			return nil
		}
	}

	p = &Pool{}
	var pool = &redis.Pool{}
	pool.MaxIdle = maxIdle
	pool.MaxActive = maxActive
	pool.Wait = true
	pool.IdleTimeout = 180 * time.Second
	pool.Dial = dialFunc
	pool.TestOnBorrow = testOnBorrow
	p.Pool = pool

	return p
}

////////////////////////////////////////////////////////////////////////////////
type Pool struct {
	*redis.Pool
}

func (this *Pool) GetSession() *Session {
	var c = this.Pool.Get()
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

func (this *Session) Conn() redis.Conn {
	return this.c
}

func (this *Session) Close() error {
	if this.c != nil {
		return this.c.Close()
	}
	return nil
}

func (this *Session) Do(commandName string, args ...interface{}) *Result {
	if this.c != nil {
		return result(this.c.Do(commandName, args...))
	}
	return result(nil, InvalidConnErr)
}

func (this *Session) Send(commandName string, args ...interface{}) *Result {
	var err = InvalidConnErr
	if this.c != nil {
		err = this.c.Send(commandName, args...)
	}
	return result(nil, err)
}

////////////////////////////////////////////////////////////////////////////////
type Conn interface {
	redis.Conn
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
