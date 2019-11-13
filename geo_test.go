package dbr

import (
	"testing"
)

func TestSession_GEOADD(t *testing.T) {
	var s = getSession()
	defer s.Close()

	s.GEOADD("g1", "13.361389", "38.115556", "m1")
	s.GEOADD("g1", "15.087269", "37.502669", "m2")
}
