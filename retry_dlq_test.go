package eventbus

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func fastRetryConfig(t *testing.T) Config {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 1
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.InitialBackoff = time.Millisecond
	cfg.Retry.MaxBackoff = 5 * time.Millisecond
	return cfg
}

// TestRetryThenDLQ：持续失败的消息必须按 MaxAttempts 重试，最终进入 DLQ，
// 且原因（普通错误 vs panic）可区分；后续消息不被阻塞。
func TestRetryThenDLQ(t *testing.T) {
	cfg := fastRetryConfig(t)
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	attempts := map[string]int{}
	var dlqSeen []DLQRecord
	processed := make(chan string, 8)

	h := func(ctx context.Context, m Message) error {
		t.Logf("handle msg=%s partition=%d offset=%d attempt=%d", m.ID, m.Partition, m.Offset, m.Attempt)
		mu.Lock()
		attempts[m.ID]++
		mu.Unlock()
		switch m.ID {
		case "fail-err":
			return errors.New("boom-error")
		case "fail-panic":
			panic("boom-panic")
		default:
			processed <- m.ID
			return nil
		}
	}
	if err := bus.Subscribe("g", h); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	for _, id := range []string{"fail-err", "ok-1", "fail-panic", "ok-2"} {
		if _, err := bus.Produce(ctx, Message{ID: id, Key: id}); err != nil {
			t.Fatalf("Produce %s: %v", id, err)
		}
	}

	// ok-1 排在 fail-err 之后，必须等 fail-err 进 DLQ 才会被处理 -> 验证有序不阻塞。
	gotOK := map[string]bool{}
	deadline := time.NewTimer(3 * time.Second)
	for len(gotOK) < 2 || len(dlqSeen) < 2 {
		select {
		case id := <-processed:
			gotOK[id] = true
			dlqSeen = bus.DLQ()
		case <-deadline.C:
			dlqSeen = bus.DLQ()
			t.Fatalf("timeout: processed=%v dlq=%d attempts=%v", gotOK, len(dlqSeen), attempts)
		}
	}

	for id, n := range attempts {
		switch id {
		case "fail-err", "fail-panic":
			if n != 3 {
				t.Errorf("%s attempted %d times, want exactly 3 (then DLQ, no silent drop)", id, n)
			}
		default:
			if n != 1 {
				t.Errorf("%s attempted %d times, want exactly 1", id, n)
			}
		}
	}

	reasons := map[string]FailReason{}
	for _, r := range dlqSeen {
		t.Logf("DLQ msg=%s offset=%d reason=%s attempts=%d lastErr=%q",
			r.Message.ID, r.Message.Offset, r.Reason, r.Attempts, r.LastError)
		if !errors.Is(r.Err(), ErrMaxRetriesExceeded) {
			t.Errorf("DLQ %s: Err()=%v does not wrap ErrMaxRetriesExceeded", r.Message.ID, r.Err())
		}
		reasons[r.Message.ID] = r.Reason
	}
	if reasons["fail-err"] != ReasonHandlerError {
		t.Errorf("fail-err reason=%q want %q", reasons["fail-err"], ReasonHandlerError)
	}
	if reasons["fail-panic"] != ReasonHandlerPanic {
		t.Errorf("fail-panic reason=%q want %q", reasons["fail-panic"], ReasonHandlerPanic)
	}
}

// TestRetrySucceedsEventually：前两次失败、第三次成功的消息只应被处理到成功为止，
// 且不进入 DLQ，位点正常提交。
func TestRetrySucceedsEventually(t *testing.T) {
	cfg := fastRetryConfig(t)
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	var mu sync.Mutex
	n := 0
	done := make(chan struct{})
	if err := bus.Subscribe("g", func(ctx context.Context, m Message) error {
		t.Logf("handle msg=%s attempt=%d", m.ID, m.Attempt)
		mu.Lock()
		n = m.Attempt
		mu.Unlock()
		if m.Attempt < 3 {
			return errors.New("transient")
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := bus.Produce(context.Background(), Message{ID: "flaky", Key: "k"}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("message never succeeded after retries")
	}
	if n != 3 {
		t.Errorf("final attempt=%d want 3", n)
	}
	if dlq := bus.DLQ(); len(dlq) != 0 {
		t.Errorf("unexpected DLQ records: %d", len(dlq))
	}
}
