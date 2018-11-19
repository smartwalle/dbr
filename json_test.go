package dbr

import (
	"fmt"
	"testing"
)

func TestSession_GETJSON(t *testing.T) {
	var rSess = getSession()
	defer rSess.Close()

	var h1 = &People{}
	h1.Name = "human"
	h1.Age = 20
	rSess.MarshalJSON("h", h1)

	var h2 *People
	rSess.UnmarshalJSON("h", &h2)
	if h2 != nil {
		fmt.Println(h2.Name, h2.Age)
	}
}
