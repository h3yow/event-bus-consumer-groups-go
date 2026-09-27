package eventbus

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentAckRaces：多个分区、多个消费组并发处理与提交位点，
// 断言同组内每条消息恰好处理一次，组间互不影响，且无数据竞争（-race）。
func TestConcurrentAckRaces(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 4
	cfg.PartitionQueueCapacity = 8
	const total = 400
	const groups = 3

	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var wgHandlers sync.WaitGroup
	finish := make(chan struct{})
	var finished int32

	for gi := 0; gi < groups; gi++ {
		group := fmt.Sprintf("g%d", gi)
		var mu sync.Mutex
		counts := map[string]int{}
		if err := bus.Subscribe(group, func(ctx context.Context, m Message) error {
			mu.Lock()
			counts[m.ID]++
			n := counts[m.ID]
			mu.Unlock()
			if n > 1 {
				t.Errorf("group=%s msg=%s processed %d times (duplicate)", group, m.ID, n)
			}
			if m.Offset%7 == 0 {
				// 模拟不同消息耗时交错，放大并发确认竞争。
				time.Sleep(100 * time.Microsecond)
			}
			return nil
		}); err != nil {
			t.Fatalf("Subscribe %s: %v", group, err)
		}
		wgHandlers.Add(1)
		go func() {
			defer wgHandlers.Done()
			waitFor(5*time.Second, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(counts) == total
			})
			mu.Lock()
			got := len(counts)
			mu.Unlock()
			if got != total {
				t.Errorf("group=%s consumed %d messages, want %d", group, got, total)
			}
			if atomic.AddInt32(&finished, 1) == groups {
				close(finish)
			}
		}()
	}

	ctx := context.Background()
	var pw sync.WaitGroup
	for i := 0; i < total; i++ {
		pw.Add(1)
		go func(i int) {
			defer pw.Done()
			id := fmt.Sprintf("msg-%04d", i)
			// 队列满是“可解释的拒绝”（消息此时已持久化到日志，不会丢失），
			// 生产者短暂退避后重试，验证背压不丢消息、不死锁。
			for {
				if _, err := bus.Produce(ctx, Message{ID: id, Key: fmt.Sprintf("key-%d", i%13)}); err != nil {
					if errors.Is(err, ErrQueueFull) {
						time.Sleep(time.Millisecond)
						continue
					}
					t.Errorf("Produce %s: %v", id, err)
				}
				return
			}
		}(i)
	}
	pw.Wait()

	select {
	case <-finish:
	case <-time.After(8 * time.Second):
		t.Fatalf("not all groups finished in time")
	}
	wgHandlers.Wait()

	// handler 全部返回后，位点提交（含 fsync）可能尚在收尾；等待其推进到日志末尾。
	if !waitFor(3*time.Second, func() bool {
		for gi := 0; gi < groups; gi++ {
			for p := 0; p < cfg.Partitions; p++ {
				off, err := bus.offsets.get(fmt.Sprintf("g%d", gi), Partition(p))
				if err != nil {
					return false
				}
				if off != bus.logs[p].endOffset()-1 {
					return false
				}
			}
		}
		return true
	}) {
		t.Fatalf("committed offsets did not reach log end")
	}

	// 所有分区的位点最终都应推进到末尾且无回退。
	for gi := 0; gi < groups; gi++ {
		for p := 0; p < cfg.Partitions; p++ {
			off, err := bus.offsets.get(fmt.Sprintf("g%d", gi), Partition(p))
			if err != nil {
				t.Fatalf("get offset: %v", err)
			}
			end := bus.logs[p].endOffset() - 1
			if off != end {
				t.Errorf("group=g%d partition=%d committed=%d want %d", gi, p, off, end)
			}
		}
	}
}

// TestGroupIsolation：一个消费组全部失败进 DLQ，另一个组正常提交，互不影响。
func TestGroupIsolation(t *testing.T) {
	cfg := fastRetryConfig(t)
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	badDone := make(chan struct{})
	goodDone := make(chan struct{})
	var badN, goodN int64

	if err := bus.Subscribe("bad", func(ctx context.Context, m Message) error {
		if atomic.AddInt64(&badN, 1) == int64(cfg.Retry.MaxAttempts) {
			select {
			case <-badDone:
			default:
				close(badDone)
			}
		}
		return fmt.Errorf("always fail")
	}); err != nil {
		t.Fatalf("Subscribe bad: %v", err)
	}
	if err := bus.Subscribe("good", func(ctx context.Context, m Message) error {
		if atomic.AddInt64(&goodN, 1) == 1 {
			close(goodDone)
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe good: %v", err)
	}

	if _, err := bus.Produce(context.Background(), Message{ID: "x", Key: "x"}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	select {
	case <-goodDone:
	case <-time.After(3 * time.Second):
		t.Fatal("good group never processed")
	}
	select {
	case <-badDone:
	case <-time.After(3 * time.Second):
		t.Fatal("bad group never exhausted retries")
	}
	// 等待 DLQ 落盘与随后的位点提交完成
	var badOff, goodOff Offset
	if !waitFor(2*time.Second, func() bool {
		if len(bus.DLQ()) != 1 {
			return false
		}
		badOff, _ = bus.offsets.get("bad", 0)
		goodOff, _ = bus.offsets.get("good", 0)
		return badOff == 0 && goodOff == 0
	}) {
		t.Fatalf("offset commit not observed: dlq=%d bad=%d good=%d",
			len(bus.DLQ()), badOff, goodOff)
	}
	if badOff != 0 {
		t.Errorf("bad group committed offset=%d want 0 (DLQ commits after exhaustion)", badOff)
	}
	if goodOff != 0 {
		t.Errorf("good group committed offset=%d want 0", goodOff)
	}
}
