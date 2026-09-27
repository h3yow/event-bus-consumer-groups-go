package eventbus

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testConfig(t *testing.T, cap2 ...int) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 2
	return cfg
}

// waitFor 轮询条件直到满足或超时。
func waitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

func TestSmokeProduceConsume(t *testing.T) {
	cfg := testConfig(t)
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	got := map[string]int{}
	if err := bus.Subscribe("g1", func(ctx context.Context, m Message) error {
		t.Logf("consume group=g1 msg=%s partition=%d offset=%d attempt=%d",
			m.ID, m.Partition, m.Offset, m.Attempt)
		mu.Lock()
		got[m.ID]++
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		id := "m-" + string(rune('a'+i))
		if _, err := bus.Produce(ctx, Message{ID: id, Key: id, Payload: []byte(id)}); err != nil {
			t.Fatalf("Produce %s: %v", id, err)
		}
	}
	if !waitFor(3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 10
	}) {
		t.Fatalf("did not consume all messages, got=%v", got)
	}
	mu.Lock()
	for id, n := range got {
		if n != 1 {
			t.Errorf("message %s processed %d times, want exactly 1", id, n)
		}
	}
	mu.Unlock()
}
