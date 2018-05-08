package dbr

import (
	"testing"
	"fmt"
)

type Human struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestSession_GETJSON(t *testing.T) {
	var s = getSession()

	var h1 = &Human{}
	h1.Name = "human"
	h1.Age = 20
	s.MarshalJSON("h", h1)

	var h2 *Human
	s.UnmarshalJSON("h", &h2)
	if h2 != nil {
		fmt.Println(h2.Name, h2.Age)
	}

	s.Close()
}
