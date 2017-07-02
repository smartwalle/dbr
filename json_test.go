package dbr

import (
	"testing"
	"fmt"
)

type Human struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestSession_GetStructWithJSON(t *testing.T) {
	var s = getSession()

	var h1 = &Human{}
	h1.Name = "human"
	h1.Age = 20
	s.SETJSON("h", h1)

	var h2 *Human
	s.GETJSON("h", &h2)
	if h2 != nil {
		fmt.Println(h2.Name, h2.Age)
	}

	s.Close()
}
