package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// shutdownConfig is a small, fast config for shutdown tests.
func shutdownConfig(dir string, drain time.Duration) Config {
	return Config{
		DataDir:       dir,
		Partitions:    2,
		QueueCapacity: 4,
		BlockPublish:  true,
		DrainTimeout:  drain,
	}
}

// TestShutdownDrainsInFlight: slow but healthy consumer, shutdown drains
// everything within the window; report matches persisted offsets.
func TestShutdownDrainsInFlight(t *testing.T) {
	b, dir := newTestBus(t, shutdownConfig("", 3*time.Second))
	logger, _ := testLogger(t)

	var mu sync.Mutex
	handled := map[string]int{}
	c, err := b.Subscribe("g1", HandlerFunc(func(ctx context.Context, m Message) error {
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		handled[m.ID]++
		mu.Unlock()
		return nil
	}), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Run(context.Background()) }()

	const n = 10
	want := map[string]bool{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("m-%02d", i)
		mustPublish(t, b, "k", id, []byte(id))
		want[id] = true
	}

	start := time.Now()
	rep, err := b.Shutdown(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	t.Logf("report: outcome=%s reason=%q elapsed=%s groups=%+v", rep.Outcome, rep.Reason, rep.Elapsed, rep.Groups)
	if rep.Outcome != OutcomeDrained {
		t.Fatalf("outcome = %s, want drained (reason %q)", rep.Outcome, rep.Reason)
	}
	if elapsed > 3*time.Second+2*time.Second {
		t.Fatalf("shutdown took %s, beyond drain window + grace", elapsed)
	}
	if len(rep.Groups) != 1 || rep.Groups[0].GroupID != "g1" {
		t.Fatalf("groups = %+v", rep.Groups)
	}
	var committed int64
	for _, ps := range rep.Groups[0].Partitions {
		if ps.Pending != 0 {
			t.Fatalf("partition %d pending = %d, want 0", ps.Partition, ps.Pending)
		}
		committed += ps.CommittedNext
	}
	if committed != n {
		t.Fatalf("committed total = %d, want %d", committed, n)
	}
	mu.Lock()
	for id := range want {
		if handled[id] != 1 {
			t.Fatalf("id=%s handled %d times, want exactly 1", id, handled[id])
		}
	}
	mu.Unlock()

	// After shutdown completes: stable, distinguishable reasons.
	if _, _, err := b.Publish("k", "late", nil); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Publish after shutdown = %v, want ErrBusClosed", err)
	}
	if _, err := b.Subscribe("g2", HandlerFunc(func(context.Context, Message) error { return nil })); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Subscribe after shutdown = %v, want ErrBusClosed", err)
	}

	// Persisted report is consistent and survives "restart".
	b2, err := New(shutdownConfig(dir, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Close() }()
	loaded, ok := b2.LastShutdown()
	if !ok {
		t.Fatal("LastShutdown after restart: no report")
	}
	if loaded.Outcome != rep.Outcome {
		t.Fatalf("persisted outcome = %s, want %s", loaded.Outcome, rep.Outcome)
	}
	for i, gs := range loaded.Groups {
		for j, ps := range gs.Partitions {
			if ps.CommittedNext != rep.Groups[i].Partitions[j].CommittedNext {
				t.Fatalf("persisted committed mismatch: %+v vs %+v", loaded.Groups, rep.Groups)
			}
		}
	}
}

// TestShutdownRejectsNewWorkDuringDrain: while draining, Publish and
// Subscribe fail with ErrBusDraining (not ErrQueueFull, not ErrBusClosed).
func TestShutdownRejectsNewWorkDuringDrain(t *testing.T) {
	b, _ := newTestBus(t, shutdownConfig("", 800*time.Millisecond))

	release := make(chan struct{})
	c, err := b.Subscribe("slow", HandlerFunc(func(ctx context.Context, m Message) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Run(context.Background()) }()
	mustPublish(t, b, "k", "m-1", []byte("x"))

	done := make(chan struct{})
	go func() { _, _ = b.Shutdown(context.Background()); close(done) }()

	// Wait until draining actually started: publish must flip to draining.
	waitFor(t, 2*time.Second, func() bool {
		_, _, err := b.Publish("k", "during-drain", nil)
		return errors.Is(err, ErrBusDraining)
	}, "Publish to return ErrBusDraining during drain")

	if _, err := b.Subscribe("late-group", HandlerFunc(func(context.Context, Message) error { return nil })); !errors.Is(err, ErrBusDraining) {
		t.Fatalf("Subscribe during drain = %v, want ErrBusDraining", err)
	}
	close(release)
	<-done

	if _, _, err := b.Publish("k", "after", nil); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Publish after shutdown = %v, want ErrBusClosed", err)
	}
}

// TestShutdownTimeoutRedeliversUncommitted: handler keeps failing with a
// backoff longer than the drain window; shutdown times out; uncommitted
// messages are redelivered after restart, committed ones are not.
func TestShutdownTimeoutRedeliversUncommitted(t *testing.T) {
	cfg := shutdownConfig("", 300*time.Millisecond)
	b, dir := newTestBus(t, cfg)
	logger, logBuf := testLogger(t)

	var mu sync.Mutex
	attempts := map[string]int{}
	c, err := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		mu.Lock()
		attempts[m.ID]++
		mu.Unlock()
		if m.ID == "good" {
			return nil
		}
		return errors.New("boom")
	}), WithMaxAttempts(100), WithBackoff(ExponentialBackoff{Base: 5 * time.Second, Max: 10 * time.Second, Multiplier: 1}), WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Run(context.Background()) }()

	// Same key -> same partition, ordered: good commits first, bad stays
	// uncommitted (stuck in a 5s backoff when shutdown hits).
	mustPublish(t, b, "k", "good", nil)
	mustPublish(t, b, "k", "bad", nil)
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts["good"] == 1 && attempts["bad"] == 1
	}, "both messages attempted")

	start := time.Now()
	rep, err := b.Shutdown(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("report: outcome=%s reason=%q elapsed=%s groups=%+v", rep.Outcome, rep.Reason, rep.Elapsed, rep.Groups)
	if rep.Outcome != OutcomeTimeout {
		t.Fatalf("outcome = %s, want timeout", rep.Outcome)
	}
	if elapsed < 300*time.Millisecond || elapsed > 300*time.Millisecond+3*time.Second {
		t.Fatalf("shutdown returned after %s, want ~drain window", elapsed)
	}
	var committed, pending int64
	for _, ps := range rep.Groups[0].Partitions {
		committed += ps.CommittedNext
		pending += int64(ps.Pending)
	}
	if committed != 1 || pending != 1 {
		t.Fatalf("committed=%d pending=%d, want 1/1", committed, pending)
	}
	if !strings.Contains(logBuf.String(), "bad") {
		t.Fatalf("expected retry logs for id=bad, got:\n%s", logBuf.String())
	}

	// Restart: only the uncommitted "bad" is redelivered; "good" is not.
	b2, err := New(shutdownConfig(dir, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Close() }()
	var mu2 sync.Mutex
	got := map[string]int{}
	c2, err := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		mu2.Lock()
		got[m.ID]++
		mu2.Unlock()
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c2.Run(context.Background()) }()
	waitFor(t, 2*time.Second, func() bool {
		mu2.Lock()
		defer mu2.Unlock()
		return got["bad"] == 1
	}, "redelivery of uncommitted message after restart")
	mu2.Lock()
	if got["good"] != 0 {
		t.Fatalf("committed message redelivered: %v", got)
	}
	mu2.Unlock()
	c2.Stop()

	// The persisted shutdown report agrees with the persisted offsets.
	loaded, ok := b2.LastShutdown()
	if !ok {
		t.Fatal("no persisted shutdown report after restart")
	}
	for _, gs := range loaded.Groups {
		for _, ps := range gs.Partitions {
			if got := b2.offsets.Get(gs.GroupID, ps.Partition); got < ps.CommittedNext {
				t.Fatalf("persisted offset %d regressed below report %d", got, ps.CommittedNext)
			}
		}
	}
}

// TestShutdownConcurrentIdempotent: many goroutines shut down at once;
// all observe the same report; repeated calls after completion replay it.
func TestShutdownConcurrentIdempotent(t *testing.T) {
	b, _ := newTestBus(t, shutdownConfig("", 2*time.Second))
	c, err := b.Subscribe("g", HandlerFunc(func(context.Context, Message) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Run(context.Background()) }()
	mustPublish(t, b, "k", "m-1", nil)

	const callers = 8
	reps := make([]ShutdownReport, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reps[i], errs[i] = b.Shutdown(context.Background())
		}(i)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if reps[i].Outcome != reps[0].Outcome || reps[i].Reason != reps[0].Reason {
			t.Fatalf("caller %d report differs: %+v vs %+v", i, reps[i], reps[0])
		}
	}
	// Repeated call after completion: same report, no error, fast.
	again, err := b.Shutdown(context.Background())
	if err != nil || again.Outcome != reps[0].Outcome {
		t.Fatalf("repeated Shutdown = %+v, %v", again, err)
	}
	// Close after Shutdown is a stable no-op error.
	if err := b.Close(); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Close after Shutdown = %v, want ErrBusClosed", err)
	}
}

// TestShutdownAfterClose: Shutdown on an already-closed bus is a stable
// distinguishable error, not a panic or half-state.
func TestShutdownAfterClose(t *testing.T) {
	b, _ := newTestBus(t, shutdownConfig("", time.Second))
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Shutdown(context.Background()); !errors.Is(err, ErrBusClosed) {
		t.Fatalf("Shutdown after Close = %v, want ErrBusClosed", err)
	}
}

// TestShutdownFullQueueBlockedProducersRetryingConsumer is the stress
// combination: bounded queue full, producers parked in Publish, consumer
// in retry backoff, and several concurrent Shutdown callers. It runs
// several rounds to shake out flakes. Assertions: shutdown returns within
// the configured bound, blocked publishers get ErrBusDraining, no message
// is lost, no message is successfully acked twice, and the report matches
// the persisted offsets.
func TestShutdownFullQueueBlockedProducersRetryingConsumer(t *testing.T) {
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			stressShutdownRound(t)
		})
	}
}

func stressShutdownRound(t *testing.T) {
	t.Helper()
	const (
		producers      = 4
		perProducer    = 10
		drainTimeout   = 3 * time.Second
		shutdownCallers = 3
	)
	b, dir := newTestBus(t, Config{
		DataDir:       "", // filled by newTestBus
		Partitions:    2,
		QueueCapacity: 4,
		BlockPublish:  true,
		DrainTimeout:  drainTimeout,
	})
	logger, _ := testLogger(t)

	// Consumer: every message fails twice (retryable) then succeeds.
	var mu sync.Mutex
	attempts := map[string]int{}
	succeeded := map[string]int{}
	c, err := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		mu.Lock()
		attempts[m.ID]++
		n := attempts[m.ID]
		mu.Unlock()
		if n <= 2 {
			return fmt.Errorf("transient %d", n)
		}
		mu.Lock()
		succeeded[m.ID]++
		mu.Unlock()
		return nil
	}), WithMaxAttempts(10),
		WithBackoff(ExponentialBackoff{Base: 2 * time.Millisecond, Max: 20 * time.Millisecond, Multiplier: 2}),
		WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Run(context.Background()) }()

	// Producers: more messages than capacity -> some park in Publish.
	type pubResult struct {
		id  string
		err error
	}
	results := make([][]pubResult, producers)
	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for i := 0; i < perProducer; i++ {
				id := fmt.Sprintf("p%d-m%02d", p, i)
				_, _, err := b.Publish("k"+id, id, []byte(id))
				results[p] = append(results[p], pubResult{id, err})
			}
		}(p)
	}

	// Let the queue fill and retries start, then shut down concurrently.
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	reps := make([]ShutdownReport, shutdownCallers)
	serrs := make([]error, shutdownCallers)
	var swg sync.WaitGroup
	for i := 0; i < shutdownCallers; i++ {
		swg.Add(1)
		go func(i int) {
			defer swg.Done()
			reps[i], serrs[i] = b.Shutdown(context.Background())
		}(i)
	}
	swg.Wait()
	elapsed := time.Since(start)
	pwg.Wait() // publishers must have been released by the seal

	for i := range serrs {
		if serrs[i] != nil {
			t.Fatalf("shutdown caller %d: %v", i, serrs[i])
		}
	}
	rep := reps[0]
	t.Logf("shutdown: outcome=%s reason=%q elapsed=%s", rep.Outcome, rep.Reason, rep.Elapsed)
	if elapsed > drainTimeout+3*time.Second {
		t.Fatalf("shutdown took %s, beyond drain window + grace", elapsed)
	}

	// Classify publish results: ok or ErrBusDraining only.
	var okIDs []string
	draining := 0
	for p := range results {
		for _, r := range results[p] {
			switch {
			case r.err == nil:
				okIDs = append(okIDs, r.id)
			case errors.Is(r.err, ErrBusDraining):
				draining++
			default:
				t.Fatalf("publish %s: unexpected error %v", r.id, r.err)
			}
		}
	}
	t.Logf("published ok=%d draining-rejected=%d total=%d", len(okIDs), draining, producers*perProducer)
	if draining == 0 {
		t.Log("note: no publisher observed ErrBusDraining (queue drained before seal)")
	}

	// The consumer had a full drain window and fast backoff: everything
	// published must have been drained and committed.
	if rep.Outcome != OutcomeDrained {
		t.Fatalf("outcome = %s (reason %q), want drained", rep.Outcome, rep.Reason)
	}
	var committed int64
	for _, gs := range rep.Groups {
		if gs.DLQCount != 0 {
			t.Fatalf("group %s DLQCount = %d, want 0", gs.GroupID, gs.DLQCount)
		}
		for _, ps := range gs.Partitions {
			if ps.Pending != 0 {
				t.Fatalf("group %s partition %d pending = %d", gs.GroupID, ps.Partition, ps.Pending)
			}
			committed += ps.CommittedNext
		}
	}
	if committed != int64(len(okIDs)) {
		t.Fatalf("committed = %d, published ok = %d", committed, len(okIDs))
	}

	// No message lost, none successfully acked twice.
	mu.Lock()
	for _, id := range okIDs {
		if succeeded[id] != 1 {
			t.Fatalf("id=%s succeeded %d times, want exactly 1", id, succeeded[id])
		}
	}
	if len(succeeded) != len(okIDs) {
		t.Fatalf("succeeded %d distinct, published ok %d", len(succeeded), len(okIDs))
	}
	mu.Unlock()

	// Report agrees with persisted state after restart; nothing redelivered.
	b2, err := New(Config{DataDir: dir, Partitions: 2, QueueCapacity: 4, BlockPublish: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Close() }()
	loaded, ok := b2.LastShutdown()
	if !ok {
		t.Fatal("no persisted shutdown report")
	}
	for i, gs := range loaded.Groups {
		for j, ps := range gs.Partitions {
			if ps.CommittedNext != rep.Groups[i].Partitions[j].CommittedNext {
				t.Fatalf("persisted report mismatch: %+v vs %+v", loaded.Groups, rep.Groups)
			}
			if got := b2.offsets.Get(gs.GroupID, ps.Partition); got != ps.CommittedNext {
				t.Fatalf("offsets.json %s/%d = %d, report says %d", gs.GroupID, ps.Partition, got, ps.CommittedNext)
			}
		}
	}
	var redelivered int
	c2, err := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		redelivered++
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = c2.Run(context.Background()); close(done) }()
	time.Sleep(100 * time.Millisecond)
	c2.Stop()
	<-done
	if redelivered != 0 {
		t.Fatalf("%d messages redelivered after fully-drained shutdown", redelivered)
	}
}
