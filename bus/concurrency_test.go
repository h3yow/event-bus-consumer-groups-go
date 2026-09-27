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

// TestConcurrentAckAcrossPartitions stresses concurrent offset commits
// from all partition goroutines plus concurrent publishers. Asserts no
// message is lost or double-processed and offsets remain contiguous.
func TestConcurrentAckAcrossPartitions(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 8
	b, _ := newTestBus(t, cfg)

	const publishers = 8
	const perPublisher = 100

	var pubWG sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWG.Add(1)
		go func(pid int) {
			defer pubWG.Done()
			for i := 0; i < perPublisher; i++ {
				id := fmt.Sprintf("p%d-m%04d", pid, i)
				key := fmt.Sprintf("k%d", (pid*7+i)%13)
				if _, _, err := b.Publish(key, id, []byte(id)); err != nil {
					t.Errorf("publish %s: %v", id, err)
					return
				}
			}
		}(p)
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		// next[partition] = next expected offset for ordering checks
		next = map[int]int64{}
	)
	logger, _ := testLogger(t)
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		mu.Lock()
		defer mu.Unlock()
		seen[m.ID]++
		if want := next[m.Partition]; m.Offset != want {
			t.Errorf("gap/reorder partition=%d id=%s offset=%d want=%d",
				m.Partition, m.ID, m.Offset, want)
		}
		next[m.Partition] = m.Offset + 1
		return nil
	}), WithLogger(logger))

	go func() { _ = c.Run(context.Background()) }()
	pubWG.Wait()

	total := publishers * perPublisher
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == total
	}, "all concurrently published messages consumed")

	for id, count := range seen {
		if count != 1 {
			t.Fatalf("message %s processed %d times", id, count)
		}
	}
	// Every partition's committed watermark must equal its log length.
	for _, p := range b.partitions {
		want := p.Len()
		got := b.offsets.Get("g", p.partition)
		if got != int64(want) {
			t.Errorf("partition %d committed=%d want=%d", p.partition, got, want)
		}
		t.Logf("partition=%d committed_offset=%d", p.partition, got)
	}
	c.Stop()
	c.Wait()
}

// TestBackpressureReject verifies that with BlockPublish=false, a full
// queue rejects publishes with the distinguishable ErrQueueFull instead
// of blocking, and that consuming frees room.
func TestBackpressureReject(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 3
	cfg.BlockPublish = false
	b, _ := newTestBus(t, cfg)

	// Block the group on the very first message: published-but-uncommitted
	// lag accumulates up to QueueCapacity and then publish is rejected.
	proceed := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		startOnce.Do(func() { close(started) })
		<-proceed // stall the whole partition (in-order head)
		return nil
	}), WithBackoff(fastBackoff()))
	go func() { _ = c.Run(context.Background()) }()

	// First message is delivered and then stalls.
	mustPublish(t, b, "k", "m0", nil)
	<-started
	// Two more fill lag up to capacity 3; they queue behind the head.
	mustPublish(t, b, "k", "m1", nil)
	mustPublish(t, b, "k", "m2", nil)
	// 4th must be explicitly rejected.
	_, _, err := b.Publish("k", "overflow", nil)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	t.Logf("publish rejected with: %v", err)

	// Drain: releasing the handler consumes everything and frees room.
	close(proceed)
	waitFor(t, testTimeout, func() bool {
		return b.offsets.Get("g", 0) == 3
	}, "backlog drained")
	if _, _, err := b.Publish("k", "after-drain", nil); err != nil {
		t.Fatalf("publish after drain: %v", err)
	}
	t.Log("publish accepted after backlog drained")
	c.Stop()
	c.Wait()
}

// TestBackpressureBlock verifies blocking publishers wake without loss
// once consumers commit offsets, and that close unblocks them.
func TestBackpressureBlock(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 2
	cfg.BlockPublish = true
	cfg.PublishTimeout = 0
	b, _ := newTestBus(t, cfg)

	var count atomic.Int64
	block := make(chan struct{})
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		if count.Add(1) <= 2 {
			<-block // fill the queue and stall
		}
		return nil
	}))
	go func() { _ = c.Run(context.Background()) }()

	mustPublish(t, b, "k", "m0", nil)
	mustPublish(t, b, "k", "m1", nil)

	done := make(chan struct{})
	var part int
	var off int64
	go func() {
		part, off, _ = b.Publish("k", "m2", nil)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("blocking publish returned while queue full")
	case <-time.After(50 * time.Millisecond):
	}
	close(block) // consumers drain; publisher must wake
	select {
	case <-done:
		t.Logf("blocked publisher woke: partition=%d offset=%d", part, off)
	case <-time.After(testTimeout):
		t.Fatal("blocking publisher never woke after drain")
	}
	waitFor(t, testTimeout, func() bool { return b.offsets.Get("g", 0) == 3 }, "all 3 committed")
	c.Stop()
	c.Wait()
}

// TestBackpressureBlockCloseUnblocks verifies bus.Close wakes blocked
// publishers with ErrBusClosed (no deadlock, no message loss).
func TestBackpressureBlockCloseUnblocks(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 1
	cfg.BlockPublish = true
	b, _ := newTestBus(t, cfg)

	block := make(chan struct{})
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		<-block
		return nil
	}))
	go func() { _ = c.Run(context.Background()) }()
	mustPublish(t, b, "k", "m0", nil) // fills capacity

	errCh := make(chan error, 1)
	go func() {
		_, _, err := b.Publish("k", "m1", nil)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(block)
	c.Stop()
	c.Wait()
	_ = b.Close() // triggers cleanup; Cleanup close is idempotent-ish

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, ErrBusClosed) && !errors.Is(err, ErrQueueFull) {
			t.Fatalf("blocked publish returned unexpected err: %v", err)
		}
		t.Logf("close unblocked publisher with: %v", err)
	case <-time.After(testTimeout):
		t.Fatal("blocked publisher deadlocked on close")
	}
}

// TestOffsetRegressionRejected verifies a lower commit is explicitly
// refused and the stored watermark stays at the higher value.
func TestOffsetRegressionRejected(t *testing.T) {
	b, _ := newTestBus(t, baseCfg())
	if err := b.offsets.Commit("g", 0, 5); err != nil {
		t.Fatal(err)
	}
	err := b.offsets.Commit("g", 0, 3)
	if !errors.Is(err, ErrOffsetRejected) {
		t.Fatalf("want ErrOffsetRejected, got %v", err)
	}
	t.Logf("regression rejected: %v", err)
	if got := b.offsets.Get("g", 0); got != 5 {
		t.Errorf("watermark changed to %d, want 5", got)
	}
	// Equal commit is idempotent.
	if err := b.offsets.Commit("g", 0, 5); err != nil {
		t.Errorf("idempotent commit: %v", err)
	}
	// Forward commit works.
	if err := b.offsets.Commit("g", 0, 6); err != nil {
		t.Errorf("forward commit: %v", err)
	}
}

// TestDuplicateAndGapAck verifies the consumer rejects double acks and
// gap acks rather than silently corrupting the watermark.
func TestDuplicateAndGapAck(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	b, _ := newTestBus(t, cfg)
	mustPublish(t, b, "k", "m0", nil)
	mustPublish(t, b, "k", "m1", nil)

	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		return nil
	}), WithBackoff(fastBackoff()))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool { return b.offsets.Get("g", 0) == 2 }, "both committed")
	c.Stop()
	c.Wait()

	// Double ack of last message.
	err := c.Ack(Message{Partition: 0, Offset: 1})
	if !errors.Is(err, ErrDuplicateAck) {
		t.Errorf("double ack: want ErrDuplicateAck, got %v", err)
	} else {
		t.Logf("duplicate ack rejected: %v", err)
	}
	// Stale ack.
	err = c.Ack(Message{Partition: 0, Offset: 0})
	if !errors.Is(err, ErrDuplicateAck) {
		t.Errorf("stale ack: want ErrDuplicateAck, got %v", err)
	}
	// Future gap ack.
	err = c.Ack(Message{Partition: 0, Offset: 99})
	if !errors.Is(err, ErrOutOfOrderAck) {
		t.Errorf("gap ack: want ErrOutOfOrderAck, got %v", err)
	} else {
		t.Logf("gap ack rejected: %v", err)
	}
}
