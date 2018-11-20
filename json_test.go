package dbr

import (
	"testing"
)

func TestSession_GETJSON(t *testing.T) {
	var rSess = getSession()
	defer rSess.Close()

	var h1 = &People{}
	h1.Name = "human"
	h1.Age = 20
	rSess.MarshalJSON("json_key1", h1)

	var h2 *People
	if err := rSess.UnmarshalJSON("json_key1", &h2); err != nil {
		t.Fatal("UnmarshalJSON 失败", err)
	}

	if h2 == nil {
		t.Fatal("UnmarshalJSON 失败")
	}

	if h2.Name != "human" || h2.Age != 20 {
		t.Fatal("UnmarshalJSON 失败")
	}
}
