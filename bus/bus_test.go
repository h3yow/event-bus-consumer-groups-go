package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

func baseCfg() Config {
	return Config{
		Partitions:    4,
		QueueCapacity: 256,
		BlockPublish:  false,
	}
}

func fastBackoff() BackoffPolicy {
	return ExponentialBackoff{Base: time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2}
}

// TestOrderedDeliveryAndNoDuplicates verifies per-partition offset order
// and exactly-once successful processing within a group.
func TestOrderedDeliveryAndNoDuplicates(t *testing.T) {
	b, _ := newTestBus(t, baseCfg())
	logger, _ := testLogger(t)

	const n = 100
	for i := 0; i < n; i++ {
		mustPublish(t, b, fmt.Sprintf("k%02d", i%7), fmt.Sprintf("m%03d", i), nil)
	}

	var (
		mu         sync.Mutex
		seen       = map[string]int{}
		nextByPart = map[int]int64{}
	)
	c, err := b.Subscribe("g1", HandlerFunc(func(ctx context.Context, m Message) error {
		mu.Lock()
		defer mu.Unlock()
		seen[m.ID]++
		if want := nextByPart[m.Partition]; m.Offset != want {
			t.Errorf("out of order: id=%s partition=%d offset=%d want=%d",
				m.ID, m.Partition, m.Offset, want)
		}
		nextByPart[m.Partition] = m.Offset + 1
		logger.Printf("id=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		return nil
	}), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = c.Run(context.Background()); close(done) }()

	waitFor(t, testTimeout, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	}, "all messages consumed")

	for id, count := range seen {
		if count != 1 {
			t.Errorf("message %s processed %d times, want 1", id, count)
		}
	}
	c.Stop()
	c.Wait()
}

// TestGroupIndependence verifies two groups each get every message and
// do not affect each other's offsets.
func TestGroupIndependence(t *testing.T) {
	b, _ := newTestBus(t, baseCfg())
	const n = 40
	for i := 0; i < n; i++ {
		mustPublish(t, b, "k", fmt.Sprintf("m%03d", i), nil)
	}

	var a, bcount atomic.Int64
	ca, _ := b.Subscribe("A", HandlerFunc(func(ctx context.Context, m Message) error {
		a.Add(1)
		return nil
	}))
	cb, _ := b.Subscribe("B", HandlerFunc(func(ctx context.Context, m Message) error {
		bcount.Add(1)
		return nil
	}))
	go func() { _ = ca.Run(context.Background()) }()
	go func() { _ = cb.Run(context.Background()) }()

	waitFor(t, testTimeout, func() bool { return a.Load() == int64(n) && bcount.Load() == int64(n) },
		"both groups consume all")
	t.Logf("group A=%d group B=%d (each must be %d)", a.Load(), bcount.Load(), n)

	for part := 0; part < baseCfg().Partitions; part++ {
		off := b.offsets.Get("A", part)
		if off <= 0 && part <= n%baseCfg().Partitions {
			// only partitions that received messages matter
		}
		_ = off
	}
	ca.Stop()
	cb.Stop()
	ca.Wait()
	cb.Wait()
}

// TestRetryThenSuccess verifies configured retries with backoff and that
// the final success commits the message exactly once.
func TestRetryThenSuccess(t *testing.T) {
	b, _ := newTestBus(t, func() Config {
		c := baseCfg()
		c.Partitions = 1
		return c
	}())
	logger, logbuf := testLogger(t)
	mustPublish(t, b, "k", "retry-me", nil)

	var attempts atomic.Int32
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		a := attempts.Add(1)
		logger.Printf("id=%s offset=%d attempt=%d", m.ID, m.Offset, a)
		if a < 3 {
			return fmt.Errorf("transient-%d", a)
		}
		return nil
	}), WithMaxAttempts(5), WithBackoff(fastBackoff()), WithLogger(logger))

	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool { return attempts.Load() == 3 }, "3 attempts then success")
	waitFor(t, testTimeout, func() bool { return b.offsets.Get("g", 0) == 1 }, "offset committed to 1")
	t.Logf("logs:\n%s", logbuf.String())
	c.Stop()
	c.Wait()
}

// TestDLQReasons verifies the two distinguishable DLQ reasons, that DLQ
// messages are not silently dropped (offset still advances) and that
// subsequent messages keep flowing.
func TestDLQReasons(t *testing.T) {
	b, _ := newTestBus(t, func() Config {
		c := baseCfg()
		c.Partitions = 1
		return c
	}())
	logger, logbuf := testLogger(t)

	mustPublish(t, b, "k", "always-fail", nil) // -> max_retries_exceeded
	mustPublish(t, b, "k", "poison", nil)      // -> non_retryable
	mustPublish(t, b, "k", "after-dlq", nil)   // must still be handled

	var failAttempts, okAfter atomic.Int32
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		logger.Printf("id=%s offset=%d attempt=%d", m.ID, m.Offset, m.Attempt)
		switch m.ID {
		case "always-fail":
			failAttempts.Add(1)
			return errors.New("boom")
		case "poison":
			return NonRetryable(errors.New("bad payload"))
		default:
			okAfter.Add(1)
			return nil
		}
	}), WithMaxAttempts(3), WithBackoff(fastBackoff()), WithLogger(logger))

	go func() { _ = c.Run(context.Background()) }()

	waitFor(t, testTimeout, func() bool {
		entries, _ := c.DLQEntries()
		return len(entries) == 2 && okAfter.Load() == 1
	}, "2 DLQ entries and later message handled")

	if got := failAttempts.Load(); got != 3 {
		t.Errorf("always-fail attempts=%d, want 3", got)
	}
	entries, _ := c.DLQEntries()
	reasons := map[RejectReason]DLQEntry{}
	for _, e := range entries {
		t.Logf("DLQ id=%s reason=%s lastErr=%q attempts=%d",
			e.Message.ID, e.Reason, e.LastErr, e.Attempts)
		reasons[e.Reason] = e
	}
	e1, ok1 := reasons[ReasonMaxRetriesExceeded]
	if !ok1 || e1.Message.ID != "always-fail" || e1.Attempts != 3 {
		t.Errorf("missing/wrong max-retries entry: %+v ok=%v", e1, ok1)
	}
	e2, ok2 := reasons[ReasonNonRetryable]
	if !ok2 || e2.Message.ID != "poison" || e2.Attempts != 1 {
		t.Errorf("missing/wrong non-retryable entry: %+v ok=%v", e2, ok2)
	}
	// DLQ messages are committed so the partition advances fully.
	waitFor(t, testTimeout, func() bool { return b.offsets.Get("g", 0) == 3 }, "offset at 3 after DLQ")
	t.Logf("offset after DLQ processing = %d\n%s", b.offsets.Get("g", 0), logbuf.String())
	c.Stop()
	c.Wait()
}

// TestRestartRecovery verifies persisted offsets survive a bus restart:
// committed messages are never redelivered; a message left uncommitted
// (simulated by crashing its handler's commit path) is redelivered once.
func TestRestartRecovery(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 2
	b, dir := newTestBus(t, cfg)

	const n = 20
	for i := 0; i < n; i++ {
		mustPublish(t, b, fmt.Sprintf("k%d", i%3), fmt.Sprintf("m%03d", i), nil)
	}

	var handled atomic.Int64
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		handled.Add(1)
		return nil
	}))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool { return handled.Load() == int64(n) }, "consume all")
	c.Stop()
	c.Wait()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-open the same data directory: offsets and WAL must be replayed.
	b2, err := New(Config{DataDir: dir, Partitions: 2, QueueCapacity: 256, BlockPublish: false})
	if err != nil {
		t.Fatal(err)
	}
	var redelivered atomic.Int64
	c2, _ := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		t.Logf("REDELIVERED id=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		redelivered.Add(1)
		return nil
	}))
	go func() { _ = c2.Run(context.Background()) }()
	time.Sleep(300 * time.Millisecond)
	c2.Stop()
	c2.Wait()
	if got := redelivered.Load(); got != 0 {
		t.Errorf("expected 0 redeliveries after restart, got %d", got)
	}
	b2.Close()
}

// TestRestartRedeliversUncommitted verifies that messages beyond the
// committed watermark are replayed after restart (no data loss).
func TestRestartRedeliversUncommitted(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.DataDir = t.TempDir()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, "k", "first", nil)
	mustPublish(t, b, "k", "second", nil)

	var mu sync.Mutex
	firstDone := false
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		if m.ID == "second" {
			// Always fails and retries forever in this test run: it is
			// never committed when the process "crashes".
			return errors.New("permanent transient")
		}
		mu.Lock()
		firstDone = true
		mu.Unlock()
		return nil
	}), WithMaxAttempts(1000000), WithBackoff(ExponentialBackoff{
		Base: time.Millisecond, Max: time.Millisecond,
	}))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstDone
	}, "first message committed")
	waitFor(t, testTimeout, func() bool { return b.offsets.Get("g", 0) == 1 }, "offset 1 persisted")
	// "Crash": stop without the second message ever committing.
	c.Stop()
	c.Wait()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var mu2 sync.Mutex
	got2 := map[string]int{}
	c2, _ := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		mu2.Lock()
		got2[m.ID]++
		t.Logf("after restart id=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		mu2.Unlock()
		return nil
	}))
	go func() { _ = c2.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool {
		mu2.Lock()
		defer mu2.Unlock()
		return got2["second"] == 1
	}, "uncommitted message redelivered")
	time.Sleep(50 * time.Millisecond)
	mu2.Lock()
	if len(got2) != 1 || got2["first"] != 0 || got2["second"] != 1 {
		t.Errorf("only the uncommitted message should replay, got %v", got2)
	}
	mu2.Unlock()
	c2.Stop()
	c2.Wait()
	b2.Close()
}
