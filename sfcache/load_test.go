package sfcache_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smartwalle/dbr"
	"github.com/smartwalle/dbr/sfcache"
)

func newTestClient(t *testing.T) dbr.UniversalClient {
	t.Helper()

	addr := os.Getenv("DBR_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "192.168.2.229:6379"
	}

	rClient, err := dbr.New(addr, "", 1, 2, 2)
	if err != nil {
		t.Skip("Redis 不可用，跳过测试:", err)
	}
	return rClient
}

func cleanKeys(ctx context.Context, rClient dbr.UniversalClient, key string) {
	rClient.Del(ctx, key, key+":empty", key+":lock")
}

func TestClient_Load(t *testing.T) {
	var rClient = newTestClient(t)
	var value, err = sfcache.Load(context.Background(), rClient, "load:1", func(ctx context.Context) ([]byte, error) {
		t.Log("开始加载数据")
		time.Sleep(time.Second)
		t.Log("数据加载完成")
		return []byte("你好!"), nil
	}, sfcache.WithExpiration(time.Second*5))

	t.Log(err)
	t.Log(string(value))
}

func TestClient_LoadHyphenValue(t *testing.T) {
	var rClient = newTestClient(t)
	var key = "load:hyphen"
	cleanKeys(context.Background(), rClient, key)

	var value []byte
	value, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		return []byte("-"), nil
	}, sfcache.WithExpiration(time.Second*5))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("-")) {
		t.Fatalf("数据不匹配，期望: -, 实际: %s", string(value))
	}

	value, err = sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		t.Fatal("不应该调用 fn，因为真实值已经写入缓存")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("-")) {
		t.Fatalf("数据不匹配，期望: -, 实际: %s", string(value))
	}
}

func TestClient_Load2(t *testing.T) {
	var rClient = newTestClient(t)
	var key = "load:2"
	cleanKeys(context.Background(), rClient, key)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			value, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
				t.Log("开始加载数据")
				time.Sleep(time.Millisecond * 140)
				t.Log("数据加载完成")
				return []byte("还是你好！"), nil
			}, sfcache.WithExpiration(time.Second*2))
			if err != nil {
				t.Errorf("%d 获取数据失败: %v", i, err)
				return
			}
			if !bytes.Equal(value, []byte("还是你好！")) {
				t.Errorf("%d 数据不匹配，实际: %s", i, string(value))
			}
		}(i)
	}
	wg.Wait()
}

func TestClient_Load3(t *testing.T) {
	var rClient = newTestClient(t)
	var key = "load:3"

	// 先删除可能存在的缓存
	cleanKeys(context.Background(), rClient, key)

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
	var rClient = newTestClient(t)
	var key = "load:negative"
	cleanKeys(context.Background(), rClient, key)

	// 1. 模拟数据不存在
	_, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		t.Log("第一次加载数据")
		return nil, sfcache.ErrNotFound
	}, sfcache.WithEmptyExpiration(time.Second*2))

	if !errors.Is(err, sfcache.ErrNotFound) {
		t.Fatalf("期望返回 sfcache.ErrNotFound，实际返回: %v", err)
	}

	// 2. 立即再次尝试，应该命中 emptyKey，不再调用 fn
	_, err = sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		t.Fatal("不应该调用 fn，因为应该命中负面缓存")
		return nil, nil
	})

	if !errors.Is(err, sfcache.ErrNotFound) {
		t.Errorf("期望返回 sfcache.ErrNotFound (负面缓存)，实际返回: %v", err)
	}

	// 3. 等待 emptyKey 过期
	time.Sleep(time.Second * 3)

	// 4. 再次尝试，emptyKey 已过期，应该重新调用 fn
	var called = false
	_, err = sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		called = true
		t.Log("第二次加载数据")
		return []byte("ok"), nil
	})

	if !called {
		t.Error("emptyKey 过期后，应该重新调用 fn")
	}
}

func TestClient_LoadSharedLoadIgnoresFirstCallerCancel(t *testing.T) {
	var rClient = newTestClient(t)
	var key = "load:shared-cancel"
	cleanKeys(context.Background(), rClient, key)

	firstCtx, firstCancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var closeStarted sync.Once
	var callCount int64

	var wg sync.WaitGroup
	var firstErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, firstErr = sfcache.Load(firstCtx, rClient, key, func(ctx context.Context) ([]byte, error) {
			atomic.AddInt64(&callCount, 1)
			closeStarted.Do(func() { close(started) })
			time.Sleep(time.Millisecond * 200)
			return []byte("ok"), nil
		}, sfcache.WithExpiration(time.Second*5), sfcache.WithLoadTimeout(time.Second))
	}()

	<-started
	firstCancel()

	value, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		t.Fatal("不应该调用第二个 fn，因为应该复用第一个加载")
		return nil, nil
	})
	wg.Wait()

	if !errors.Is(firstErr, context.Canceled) {
		t.Fatalf("第一个调用期望返回 context.Canceled，实际: %v", firstErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("ok")) {
		t.Fatalf("数据不匹配，期望: ok，实际: %s", string(value))
	}
	if callCount != 1 {
		t.Fatalf("期望 fn 只执行 1 次，实际: %d", callCount)
	}
}

func TestClient_LoadTimeoutNotFoundDoesNotWriteEmptyCache(t *testing.T) {
	var rClient = newTestClient(t)
	var key = "load:timeout-not-found"
	cleanKeys(context.Background(), rClient, key)

	_, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		time.Sleep(time.Millisecond * 100)
		return nil, sfcache.ErrNotFound
	}, sfcache.WithLoadTimeout(time.Millisecond*20))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望返回 context.DeadlineExceeded，实际: %v", err)
	}

	var called bool
	value, err := sfcache.Load(context.Background(), rClient, key, func(ctx context.Context) ([]byte, error) {
		called = true
		return []byte("ok"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("超时后的 ErrNotFound 不应写入 emptyKey，后续请求应该重新调用 fn")
	}
	if !bytes.Equal(value, []byte("ok")) {
		t.Fatalf("数据不匹配，期望: ok，实际: %s", string(value))
	}
}
