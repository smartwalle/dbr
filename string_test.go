package dbr

import (
	"testing"
)

func TestSession_SET(t *testing.T) {
	var s = getSession()
	defer s.Close()
	if r := s.SET("k1", "v", "EX", 10, "NX"); r.Error != nil {
		t.Fatal("SET With EX NX 指令错误", r.Error)
	}
}
