package dbr

import (
	"testing"
)

func TestSession_BeginTx(t *testing.T) {
	var rSess = getSession()
	defer rSess.Close()

	rSess.BeginTx()
	rSess.Send("SET", "key1", "1000")
	rSess.Send("SET", "key2", "2000")
	if r := rSess.Commit(); r.Error != nil {
		t.Fatal("EXEC 指令错误", r.Error)
	}
}
