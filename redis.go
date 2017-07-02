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

func (this *Session) Do(commandName string, args ...interface{}) (interface{}, error) {
	return this.c.Do(commandName, args...)
}

////////////////////////////////////////////////////////////////////////////////
func (this *Session) Transaction(callback func(conn Conn)) (reply interface{}, err error) {
	var c = this.c
	c.Send("MULTI")
	callback(c)
	return c.Do("EXEC")
}

func (this *Session) Pipeline(callback func(conn Conn)) (err error) {
	var c = this.c
	callback(c)
	return c.Flush()
}

////////////////////////////////////////////////////////////////////////////////
func (this *Session) Bytes(reply interface{}, err error) ([]byte, error) {
	return Bytes(reply, err)
}

func (this *Session) Int(reply interface{}, err error) (int, error) {
	return Int(reply, err)
}

func (this *Session) Ints(reply interface{}, err error) ([]int, error) {
	return Ints(reply, err)
}

func (this *Session) Int64(reply interface{}, err error) (int64, error) {
	return Int64(reply, err)
}

func (this *Session) Bool(reply interface{}, err error) (bool, error) {
	return Bool(reply, err)
}

func (this *Session) String(reply interface{}, err error) (string, error) {
	return String(reply, err)
}

func (this *Session) Strings(reply interface{}, err error) ([]string, error) {
	return Strings(reply, err)
}

func (this *Session) Float64(reply interface{}, err error) (float64, error) {
	return Float64(reply, err)
}

func (this *Session) MustBytes(reply interface{}, err error) []byte {
	var r, _ = Bytes(reply, err)
	return r
}

func (this *Session) MustInt(reply interface{}, err error) int {
	var r, _ = Int(reply, err)
	return r
}

func (this *Session) MustInts(reply interface{}, err error) []int {
	var r, _ = Ints(reply, err)
	return r
}

func (this *Session) MustInt64(reply interface{}, err error) int64 {
	var r, _ = Int64(reply, err)
	return r
}

func (this *Session) MustBool(reply interface{}, err error) bool {
	var r, _ = Bool(reply, err)
	return r
}

func (this *Session) MustString(reply interface{}, err error) string {
	var r, _ = String(reply, err)
	return r
}

func (this *Session) MustStrings(reply interface{}, err error) []string {
	var r, _ = Strings(reply, err)
	return r
}

func (this *Session) MustFloat64(reply interface{}, err error) float64 {
	var r, _ = Float64(reply, err)
	return r
}

func (this *Session) ScanStruct(source, destination interface{}) error {
	var err = redigo.ScanStruct(source.([]interface{}), destination)
	return err
}

func (this *Session) StructToArgs(key string, obj interface{}) (redigo.Args) {
	return redigo.Args{}.Add(key).AddFlat(obj)
}

////////////////////////////////////////////////////////////////////////////////
func Bytes(reply interface{}, err error) ([]byte, error) {
	return redigo.Bytes(reply, err)
}

func Int(reply interface{}, err error) (int, error) {
	return redigo.Int(reply, err)
}

func Ints(reply interface{}, err error) ([]int, error) {
	return redigo.Ints(reply, err)
}

func Int64(reply interface{}, err error) (int64, error) {
	return redigo.Int64(reply, err)
}

func Bool(reply interface{}, err error) (bool, error) {
	return redigo.Bool(reply, err)
}

func String(reply interface{}, err error) (string, error) {
	return redigo.String(reply, err)
}

func Strings(reply interface{}, err error) ([]string, error) {
	return redigo.Strings(reply, err)
}

func Float64(reply interface{}, err error) (float64, error) {
	return redigo.Float64(reply, err)
}

func MustBytes(reply interface{}, err error) []byte {
	var r, _ = Bytes(reply, err)
	return r
}

func MustInt(reply interface{}, err error) int {
	var r, _ = Int(reply, err)
	return r
}

func MustInts(reply interface{}, err error) []int {
	var r, _ = Ints(reply, err)
	return r
}

func MustInt64(reply interface{}, err error) int64 {
	var r, _ = Int64(reply, err)
	return r
}

func MustBool(reply interface{}, err error) bool {
	var r, _ = Bool(reply, err)
	return r
}

func MustString(reply interface{}, err error) string {
	var r, _ = String(reply, err)
	return r
}

func MustStrings(reply interface{}, err error) []string {
	var r, _ = Strings(reply, err)
	return r
}

func MustFloat64(reply interface{}, err error) float64 {
	var r, _ = Float64(reply, err)
	return r
}

func ScanStruct(source, destination interface{}) error {
	var err = redigo.ScanStruct(source.([]interface{}), destination)
	return err
}

func StructToArgs(key string, obj interface{}) (redigo.Args) {
	return redigo.Args{}.Add(key).AddFlat(obj)
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
