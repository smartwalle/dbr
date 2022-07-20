package dbr_test

import (
	"context"
	"github.com/smartwalle/dbr"
	"testing"
	"time"
)

func BenchmarkClient_GetBlock(b *testing.B) {
	var rClient, _ = dbr.New("192.168.1.77:6379", "", 1, 2, 2)

	for i := 0; i < b.N; i++ {
		var block, value, err = rClient.GetBlock(context.Background(), "k1")
		if block {
			b.Log("block..")
			continue
		}

		if err != nil {
			b.Log("error:", err)
			continue
		}

		if value != "" {
			// 如果有值，则表示从缓存读取数据成功，直接处理数据即可
			//b.Log(i, "成功读取数据:", value)
			continue
		}

		b.Log("Write...")

		// 如果没有值，从其它数据源获取数据，然后写入数据
		rClient.Set(context.Background(), "k1", i, time.Minute*10)
	}
}
