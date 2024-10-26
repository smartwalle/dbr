package dbr_test

import (
	"context"
	"github.com/smartwalle/dbr"
	"sync"
	"testing"
	"time"
)

type Person struct {
	Name string
}

func TestFetch(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)
	var value, err = dbr.Fetch(context.Background(), rClient, "fetch:1", func(ctx context.Context) (string, time.Duration, error) {
		t.Log("开始加载数据")
		time.Sleep(time.Second)
		t.Log("数据加载完成")
		return "你好!", time.Second * 5, nil
	})

	t.Log(err)
	t.Log(value)
}

func TestFetch2(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			value, err := dbr.Fetch(context.Background(), rClient, "fetch:2", func(ctx context.Context) (string, time.Duration, error) {
				t.Log("开始加载数据")
				time.Sleep(time.Millisecond * 120)
				t.Log("数据加载完成")
				return "还是你好！", time.Second * 5, nil
			})
			if err != nil {
				t.Log(err)
			}
			if value != "还是你好" {
				t.Log(value)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
