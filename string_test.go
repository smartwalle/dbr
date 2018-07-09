package dbr

import (
	"fmt"
	"testing"
)

func TestSession_SET(t *testing.T) {
	fmt.Println(getSession().SET("k1", "v", "EX", 10, "NX").MustString())
}
