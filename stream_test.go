package dbr

import (
	"fmt"
	"testing"
)

func TestSession_XADD(t *testing.T) {
	var sSess = getSession()
	defer sSess.Close()

	if r := sSess.XADD("ms", 20, "*", "f1", "v1", "", "", "", ""); r.Error != nil {
		t.Fatal("XADD 指令错误", r.Error)
	}
}

func TestSession_XREAD(t *testing.T) {
	var sSess = getSession()
	defer sSess.Close()

	if r := sSess.XREAD(0, 0, "ms", "0-0"); r.Error != nil {
		t.Fatal("XREAD 指令错误", r.Error)
	}
}

func TestSession_XGROUPCREATE(t *testing.T) {
	var sSess = getSession()
	defer sSess.Close()

	if r := sSess.XGROUPCREATE("ms", "g1", "0"); r.Error != nil {
		t.Fatal("XGROUP CREATE 指令错误", r.Error)
	}
}

func TestSession_XREADGROUP(t *testing.T) {
	var sSess = getSession()
	defer sSess.Close()

	var r *Result
	if r = sSess.XREADGROUP("g1", "c1", 0, 0, "ms", "0"); r.Error != nil {
		t.Fatal("XREAD 指令错误", r.Error)
	}

	var sList = r.MustStreams()

	for _, s := range sList {
		fmt.Println(s.Key, s.Id)
		for _, f := range s.Fields {
			fmt.Println("--", f.Field, f.Value)
		}
	}
}
