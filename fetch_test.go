package dbr_test

import (
	"bytes"
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
	var value, err = dbr.Fetch(context.Background(), rClient, "fetch:1", func(ctx context.Context) ([]byte, error) {
		t.Log("开始加载数据")
		time.Sleep(time.Second)
		t.Log("数据加载完成")
		return []byte("你好!"), nil
	}, dbr.WithExpiration(time.Second*5))

	t.Log(err)
	t.Log(value)
}

func TestFetch2(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			value, err := dbr.Fetch(context.Background(), rClient, "fetch:2", func(ctx context.Context) ([]byte, error) {
				t.Log("开始加载数据")
				time.Sleep(time.Millisecond * 100)
				t.Log("数据加载完成")
				return []byte("还是你好！"), nil
			}, dbr.WithExpiration(time.Second*5))
			if err != nil {
				t.Log(err)
			}
			if !bytes.Equal(value, []byte("还是你好！")) {
				t.Log(value)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
