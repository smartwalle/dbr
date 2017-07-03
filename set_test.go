package dbr

import "testing"

func TestSession_SADD(t *testing.T) {
	var s = getSession()
	if r := s.SADD("set1", "1", "2", "3"); r.Error != nil {
		t.Fatal("SADD 指令错误", r.Error)
	}
	s.Close()
}

func TestSession_SCARD(t *testing.T) {
	var s = getSession()
	if s.SCARD("set1").MustInt64() == 0 {
		t.Fatal("SCARD 指令错误")
	}
	if s.SCARD("set2").MustInt64() != 0 {
		t.Fatal("SCART 指令错误")
	}
	s.Close()
}