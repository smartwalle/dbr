package dbr

import (
	"fmt"
	"testing"
)

func TestSession_Receive(t *testing.T) {
	var rSess = getSession()
	rSess.Send("SET", "test_key", 10)
	rSess.Send("GET", "test_key")
	rSess.Flush()
	fmt.Println(rSess.Receive())
	fmt.Println(rSess.Receive().Int())
}
