package dbr_test

import (
	"bytes"
	"context"
	"github.com/smartwalle/dbr"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClient_Load(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)
	var value, err = dbr.Load(context.Background(), rClient, "load:1", func(ctx context.Context) ([]byte, error) {
		t.Log("开始加载数据")
		time.Sleep(time.Second)
		t.Log("数据加载完成")
		return []byte("你好!"), nil
	}, dbr.WithExpiration(time.Second*5))

	t.Log(err)
	t.Log(value)
}

func TestClient_Load2(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			value, err := dbr.Load(context.Background(), rClient, "load:2", func(ctx context.Context) ([]byte, error) {
				t.Log("开始加载数据")
				time.Sleep(time.Millisecond * 500)
				t.Log("数据加载完成")
				return []byte("还是你好！"), nil
			}, dbr.WithExpiration(time.Second*2))
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

func TestClient_Load3(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)
	var key = "load:3"

	// 先删除可能存在的缓存
	rClient.Del(context.Background(), key)

	var callCount int64
	var wg sync.WaitGroup
	var concurrency = 100

	var successCount int64
	var failedCount int64

	// 模拟大量并发请求同时访问一个不存在的缓存
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := dbr.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
				// 增加调用计数
				atomic.AddInt64(&callCount, 1)
				t.Log("开始加载数据，当前调用次数:", atomic.LoadInt64(&callCount))
				time.Sleep(time.Millisecond * 200) // 模拟较长的数据加载时间
				t.Log("-----", time.Now().UnixMilli())
				return []byte("缓存击穿保护测试数据"), nil
			},
				dbr.WithExpiration(time.Second*100),
				dbr.WithMaxAttempts(5),
				dbr.WithRetryDelay(time.Millisecond*20),
			)

			if err != nil {
				t.Errorf("获取数据失败: %v", err)
				atomic.AddInt64(&failedCount, 1)
				return
			}
			if !bytes.Equal(value, []byte("缓存击穿保护测试数据")) {
				atomic.AddInt64(&failedCount, 1)
				t.Errorf("数据不匹配，期望: %s, 实际: %s   %d", "缓存击穿保护测试数据", string(value), time.Now().UnixMilli())
				return
			}

			atomic.AddInt64(&successCount, 1)
		}()
	}
	wg.Wait()

	// 验证数据源函数只被调用了一次（只有获取到锁的请求才会调用）
	if callCount != 1 {
		t.Errorf("缓存击穿保护失败，期望调用次数: 1, 实际调用次数: %d", callCount)
	} else {
		t.Log("缓存击穿保护测试通过，数据源只被调用了一次")
	}

	t.Log(failedCount, successCount)
}
