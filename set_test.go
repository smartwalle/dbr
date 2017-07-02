package dbr

import "testing"

func TestSession_SADD(t *testing.T) {
	var s = getSession()
	if _, err := s.SADD("set1", "1", "2", "3"); err != nil {
		t.Fatal("SADD 指令错误", err)
	}
	s.Close()
}

func TestSession_SCARD(t *testing.T) {
	var s = getSession()
	if s.SCARD("set1") == 0 {
		t.Fatal("SCARD 指令错误")
	}
	if s.SCARD("set2") != 0 {
		t.Fatal("SCART 指令错误")
	}
	s.Close()
}