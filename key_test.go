package dbr

import (
	"testing"
	"time"
)

func TestSession_DEL(t *testing.T) {
	var s = getSession()
	s.SET("del_key", "del_value")
	if _, err := Int64(s.DEL("del_key")); err != nil {
		t.Fatal("DEL 指令错误", err)
	}
	s.Close()
}

func TestSession_EXISTS(t *testing.T) {
	var s = getSession()
	s.SET("exists_key", "exists_value")
	if s.EXISTS("exists_key") == false {
		t.Fatal("EXISTS 指令错误")
	}
	if s.EXISTS("not_exists_key") == true {
		t.Fatal("EXISTS 指令错误")
	}
	s.Close()
}

func TestSession_EXPIRE(t *testing.T) {
	var s = getSession()
	s.SET("3s", "3 秒后过期")

	if _, err := s.EXPIRE("3s", 3); err != nil {
		t.Fatal("EXPIRE 指令错误", err)
	}

	var timer = time.NewTimer(time.Second * 4)
	<-timer.C
	if s.EXISTS("3s") == true {
		t.Fatal("EXPIRE 指令无效")
	}

	s.Close()
}

func TestSession_EXPIREAT(t *testing.T) {
	var s = getSession()
	s.SET("2s", "2 秒后过期")

	if _, err := s.EXPIREAT("2s", time.Now().Unix() + 2); err != nil {
		t.Fatal("EXPIREAT 指令错误", err)
	}

	var timer = time.NewTimer(time.Second * 3)
	<-timer.C
	if s.EXISTS("2s") == true {
		t.Fatal("EXPIREAT 指令无效")
	}

	s.Close()
}