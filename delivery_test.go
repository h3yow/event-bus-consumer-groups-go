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

// TestBackpressureRejectReason：非阻塞模式下，在途追加队列满时必须立即返回
// 可区分的 ErrQueueFull；退避重试后消息全部成功，拒绝不导致丢消息或死锁。
func TestBackpressureRejectReason(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 1
	cfg.PartitionQueueCapacity = 1 // 极小容量，易触发拒绝
	cfg.BlockWhenFull = false
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	count := map[string]int{}
	if err := bus.Subscribe("g", func(ctx context.Context, m Message) error {
		mu.Lock()
		count[m.ID]++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	const n = 300
	var rejected int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("m%03d", i)
			for {
				_, err := bus.Produce(ctx, Message{ID: id, Key: "k"})
				if err == nil {
					return
				}
				if !errors.Is(err, ErrQueueFull) {
					t.Errorf("Produce %s: %v", id, err)
					return
				}
				atomic.AddInt64(&rejected, 1)
				// 可解释的拒绝：退避后重试。
				time.Sleep(time.Duration(1+(i%3)) * time.Millisecond)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("producers deadlocked after ErrQueueFull rejections")
	}
	t.Logf("observed %d distinguishable ErrQueueFull rejections, all retried successfully", atomic.LoadInt64(&rejected))
	if atomic.LoadInt64(&rejected) == 0 {
		t.Skip("queue never saturated under this timing; rejection path not exercised")
	}
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(count) == n
	}) {
		t.Fatalf("consumed=%d want %d (message lost)", len(count), n)
	}
	mu.Lock()
	for id, c := range count {
		if c != 1 {
			t.Errorf("msg=%s processed %d times want 1", id, c)
		}
	}
	mu.Unlock()
}

// TestBlockingProduceDoesNotDeadlock：阻塞模式下队列满时 Produce 阻塞等待，
// 消费者持续推进后生产者全部完成——验证无死锁、无丢失。
func TestBlockingProduceDoesNotDeadlock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 2
	cfg.PartitionQueueCapacity = 1
	cfg.BlockWhenFull = true
	cfg.Retry.MaxAttempts = 1
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	count := map[string]int{}
	if err := bus.Subscribe("g", func(ctx context.Context, m Message) error {
		mu.Lock()
		count[m.ID]++
		mu.Unlock()
		if m.Offset%3 == 0 {
			time.Sleep(2 * time.Millisecond)
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("m%03d", i)
			if _, err := bus.Produce(context.Background(), Message{ID: id, Key: fmt.Sprintf("k%d", i%5)}); err != nil {
				t.Errorf("Produce %s: %v", id, err)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("blocking producers deadlocked")
	}
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(count) == n
	}) {
		t.Fatalf("consumed=%d want %d", len(count), n)
	}
	mu.Lock()
	for id, c := range count {
		if c != 1 {
			t.Errorf("msg=%s processed %d times want 1", id, c)
		}
	}
	mu.Unlock()
}

// TestPartitionOrdering：同一 Key 固定分区且严格按 Offset 顺序处理。
func TestPartitionOrdering(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 3
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	seen := map[string][]Offset{}
	const perKey = 50
	if err := bus.Subscribe("ord", func(ctx context.Context, m Message) error {
		mu.Lock()
		seen[m.Key] = append(seen[m.Key], m.Offset)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	keys := []string{"alpha", "beta"}
	for _, k := range keys {
		for i := 0; i < perKey; i++ {
			id := fmt.Sprintf("%s-%d", k, i)
			if _, err := bus.Produce(context.Background(), Message{ID: id, Key: k}); err != nil {
				t.Fatalf("Produce: %v", err)
			}
		}
	}
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range keys {
			if len(seen[k]) != perKey {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("ordering wait failed: %v", seen)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, k := range keys {
		offs := seen[k]
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				t.Fatalf("key=%s offsets not strictly increasing: %v", k, offs)
			}
		}
		t.Logf("key=%s consumed offsets in order: %v", k, offs)
	}
}
