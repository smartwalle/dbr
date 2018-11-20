package dbr

import (
	"testing"
)

func TestSession_HMSET(t *testing.T) {
	var s = getSession()
	defer s.Close()
	if r := s.HMSET("hmset_key1", "hk1", "hv1", "hk2", "hv2"); r.Error != nil {
		t.Fatal("HMSET 指令错误", r.Error)
	}
}

func TestSession_HGETALL(t *testing.T) {
	var s = getSession()
	defer s.Close()
	if r := s.HGETALL("hmset_key1"); r.Error != nil {
		t.Fatal("HGETALL 指令错误", r.Error)
	}
}
