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
	fmt.Println("Pipe SET", rSess.Receive().MustString())
	fmt.Println("Pipe GET", rSess.Receive().MustInt())
}
