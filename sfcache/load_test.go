package sfcache_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smartwalle/dbr"
	"github.com/smartwalle/dbr/sfcache"
)

func TestClient_Load(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)
	var value, err = sfcache.Load(context.Background(), rClient, "load:1", func(ctx context.Context) ([]byte, error) {
		t.Log("开始加载数据")
		time.Sleep(time.Second)
		t.Log("数据加载完成")
		return []byte("你好!"), nil
	}, sfcache.WithExpiration(time.Second*5))

	t.Log(err)
	t.Log(string(value))
}

func TestClient_Load2(t *testing.T) {
	var rClient, _ = dbr.New("127.0.0.1:6379", "", 1, 2, 2)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			value, err := sfcache.Load(context.Background(), rClient, "load:2", func(ctx context.Context) ([]byte, error) {
				t.Log("开始加载数据")
				time.Sleep(time.Millisecond * 140)
				t.Log("数据加载完成")
				return []byte("还是你好！"), nil
			}, sfcache.WithExpiration(time.Second*2))
			if err != nil {
				t.Log(i, err)
			}
			if !bytes.Equal(value, []byte("还是你好！")) {
				t.Log(i, value)
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
	var concurrency = 1000

	var successCount int64
	var failedCount int64

	// 模拟大量并发请求同时访问一个不存在的缓存
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
				// 增加调用计数
				atomic.AddInt64(&callCount, 1)
				t.Log("开始加载数据，当前调用次数:", atomic.LoadInt64(&callCount))
				time.Sleep(time.Millisecond * 200) // 模拟较长的数据加载时间
				return []byte("缓存击穿保护测试数据"), nil
			},
				sfcache.WithExpiration(time.Second*100),
				sfcache.WithMaxAttempts(5),
				sfcache.WithRetryDelay(time.Millisecond*100),
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

func TestClient_LoadNegativeCache(t *testing.T) {
	var rClient, err = dbr.New("127.0.0.1:6379", "", 1, 2, 2)
	if err != nil {
		t.Skip("Redis 不可用，跳过测试:", err)
	}

	var key = "load:negative"
	rClient.Del(context.Background(), key)

	// 1. 模拟加载失败
	_, err = sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		t.Log("第一次加载数据")
		return nil, errors.New("读取数据异常") // 模拟加载失败
	}, sfcache.WithPlaceholderExpiration(time.Second*2))

	if err == nil {
		t.Fatal("期望加载失败，但成功了")
	}

	// 2. 立即再次尝试，应该命中“负面缓存”占位符，返回 redis.Nil
	_, err = sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		t.Fatal("不应该调用 fn，因为应该命中负面缓存")
		return nil, nil
	})

	if !errors.Is(err, sfcache.ErrHitPlaceholder) {
		t.Errorf("期望返回 sfcache.ErrHitPlaceholder (负面缓存)，实际返回: %v", err)
	}

	// 3. 等待占位符过期
	time.Sleep(time.Second * 3)

	// 4. 再次尝试，占位符已过期，应该重新调用 fn
	var called = false
	_, err = sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		called = true
		t.Log("第二次加载数据")
		return []byte("ok"), nil
	})

	if !called {
		t.Error("占位符过期后，应该重新调用 fn")
	}
}
