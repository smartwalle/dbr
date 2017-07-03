package dbr

import (
	redigo "github.com/garyburd/redigo/redis"
)

type Result struct {
	Data  interface{}
	Error error
}

func result(data interface{}, err error) (*Result) {
	return &Result{data, err}
}

////////////////////////////////////////////////////////////////////////////////
func (this *Result) Bytes() ([]byte, error) {
	if this.Error != nil {
		return nil, this.Error
	}
	return redigo.Bytes(this.Data, this.Error)
}

func (this *Result) Int() (int, error) {
	if this.Error != nil {
		return 0, this.Error
	}
	return redigo.Int(this.Data, this.Error)
}

func (this *Result) Ints() ([]int, error) {
	if this.Error != nil {
		return nil, this.Error
	}
	return redigo.Ints(this.Data, this.Error)
}

func (this *Result) Int64() (int64, error) {
	if this.Error != nil {
		return 0, this.Error
	}
	return redigo.Int64(this.Data, this.Error)
}

func (this *Result) Bool() (bool, error) {
	if this.Error != nil {
		return false, this.Error
	}
	return redigo.Bool(this.Data, this.Error)
}

func (this *Result) String() (string, error) {
	if this.Error != nil {
		return "", this.Error
	}
	return redigo.String(this.Data, this.Error)
}

func (this *Result) Strings() ([]string, error) {
	if this.Error != nil {
		return nil, this.Error
	}
	return redigo.Strings(this.Data, this.Error)
}

func (this *Result) Float64() (float64, error) {
	if this.Error != nil {
		return 0.0, this.Error
	}
	return redigo.Float64(this.Data, this.Error)
}

////////////////////////////////////////////////////////////////////////////////
func (this *Result) MustBytes() []byte {
	var r, _ = this.Bytes()
	return r
}

func (this *Result) MustInt() int {
	var r, _ = this.Int()
	return r
}

func (this *Result) MustInts() []int {
	var r, _ = this.Ints()
	return r
}

func (this *Result) MustInt64() int64 {
	var r, _ = this.Int64()
	return r
}

func (this *Result) MustBool() bool {
	var r, _ = this.Bool()
	return r
}

func (this *Result) MustString() string {
	var r, _ = this.String()
	return r
}

func (this *Result) MustStrings() []string {
	var r, _ = this.Strings()
	return r
}

func (this *Result) MustFloat64() float64 {
	var r, _ = this.Float64()
	return r
}

////////////////////////////////////////////////////////////////////////////////
func (this *Result) ScanStruct(source, destination interface{}) error {
	var err = redigo.ScanStruct(source.([]interface{}), destination)
	return err
}

func (this *Result) StructToArgs(key string, obj interface{}) (redigo.Args) {
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