package dbr

import (
	"testing"
)

func TestSession_Receive(t *testing.T) {
	var rSess = getSession()
	defer rSess.Close()

	rSess.Send("SET", "test_key", 10)
	rSess.Send("GET", "test_key")
	rSess.Flush()

	if r := rSess.Receive(); r.Error != nil {
		t.Fatal("Pipe SET 指令错误", r.Error)
	}
	if r := rSess.Receive(); r.Error != nil {
		t.Fatal("Pipe GET 指令错误", r.Error)
	}
}
