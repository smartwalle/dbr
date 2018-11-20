package dbr

import (
	"fmt"
	"testing"
)

func TestSession_WithBlock(t *testing.T) {
	var rSess = getSession()
	defer rSess.Close()

	var pKey = "p2"

	var block, rResult = rSess.WithBlock(pKey, "null", 30)
	if block {
		// 判断是否需要 block
		fmt.Println("信息不存在")
		return
	}

	var p *People
	if rResult != nil && rResult.Error == nil {
		if err := rResult.UnmarshalJSON(&p); err == nil {
			fmt.Println("获取到数据", p.Name, p.Age)
			return
		}
	}

	//p = &People{"people_1", 100}
	//rSess.MarshalJSONEx(pKey, 30, p)
	//fmt.Println("新建数据", p.Name, p.Age)
}
