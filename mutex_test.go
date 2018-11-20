package dbr

import (
	"testing"
)

func TestMutex_Lock(t *testing.T) {
	var rs = NewRedSync(getPool())

	var mu = rs.NewMutex("test_mutex")
	if err := mu.Lock(); err != nil {
		t.Fatal("加锁失败", err)
	}

	if mu.Unlock() == false {
		t.Fatal("解锁失败")
	}
}
