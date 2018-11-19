package dbr

import (
	"fmt"
	"testing"
)

func TestSession_BeginTx(t *testing.T) {
	var rSess = getSession()
	defer rSess.Close()

	rSess.BeginTx()
	rSess.Send("SET", "key1", "1000")
	rSess.Send("SET", "key2", "2000")
	fmt.Println("Tx Commit", rSess.Commit().MustStrings())
}
