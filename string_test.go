package dbr

import (
	"testing"
	"fmt"
)

func TestSession_MSET(t *testing.T) {
	var s = getSession()
	s.MSET("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4")
	fmt.Println(Strings(s.MGET("k1", "k2")))
	s.Close()
}