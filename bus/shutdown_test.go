package bus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownDrainsWithinWindow verifies the happy path: with a healthy
// consumer, Shutdown stops accepting new work, lets in-flight processing
// finish, persists every commit and reports drained with zero pending.
func TestShutdownDrainsWithinWindow(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 2
	cfg.BlockPublish = false
	b, dir := newTestBus(t, cfg)
	logger, _ := testLogger(t)

	const n = 20
	for i := 0; i < n; i++ {
		mustPublish(t, b, fmt.Sprintf("k%d", i%2), fmt.Sprintf("m%02d", i), nil)
	}

	var seen atomic.Int64
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		seen.Add(1)
		logger.Printf("HANDLE id=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		return nil
	}), WithLogger(logger))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool { return seen.Load() == n }, "all consumed")

	rep, err := b.Shutdown(WithDrainTimeout(2*time.Second), WithStopGrace(time.Second))
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if rep.Outcome != OutcomeDrained {
		t.Fatalf("outcome=%s reason=%s, want drained", rep.Outcome, rep.Reason)
	}
	var totalPending, totalNewDLQ int64
	for _, g := range rep.Groups {
		if g.GroupID != "g" {
			continue
		}
		totalPending += g.Pending
		for _, ps := range g.Partitions {
			t.Logf("report group=%s partition=%d committed_next=%d end=%d pending=%d newDLQ=%d",
				g.GroupID, ps.Partition, ps.CommittedNext, ps.LogEndOffset, ps.Pending, ps.NewDLQ)
		}
		totalNewDLQ += int64(g.NewDLQ)
	}
	if totalPending != 0 {
		t.Errorf("pending=%d after drain, want 0", totalPending)
	}
	if totalNewDLQ != 0 {
		t.Errorf("newDLQ=%d, want 0", totalNewDLQ)
	}

	// After shutdown: publish/subscribe return stable closed errors.
	if _, _, err := b.Publish("k", "late", nil); !errors.Is(err, ErrBusClosed) {
		t.Errorf("publish after shutdown err=%v, want ErrBusClosed", err)
	}
	if _, err := b.Subscribe("g2", HandlerFunc(func(context.Context, Message) error { return nil })); !errors.Is(err, ErrBusClosed) {
		t.Errorf("subscribe after shutdown err=%v, want ErrBusClosed", err)
	}

	// Reopen: committed positions must equal the report (0 redelivery).
	b2, err := New(Config{DataDir: dir, Partitions: 2, QueueCapacity: 256, BlockPublish: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b2.Close() })
	last := b2.LastShutdownReport()
	if last == nil || last.Outcome != OutcomeDrained {
		t.Fatalf("reloaded report=%+v, want drained", last)
	}
	for _, g := range last.Groups {
		for _, ps := range g.Partitions {
			got := b2.offsets.Get(g.GroupID, ps.Partition)
			if got != ps.CommittedNext {
				t.Errorf("after restart group=%s partition=%d offset=%d report=%d",
					g.GroupID, ps.Partition, got, ps.CommittedNext)
			}
		}
	}
	var redelivered atomic.Int64
	c2, _ := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		t.Logf("UNEXPECTED redelivery id=%s offset=%d", m.ID, m.Offset)
		redelivered.Add(1)
		return nil
	}))
	go func() { _ = c2.Run(context.Background()) }()
	time.Sleep(200 * time.Millisecond)
	c2.Stop()
	c2.Wait()
	if redelivered.Load() != 0 {
		t.Errorf("%d redeliveries after a fully-drained shutdown", redelivered.Load())
	}
}

// TestShutdownTimeoutFencesCommit verifies that when the drain window is
// too short for a retrying head message, Shutdown returns within a bound,
// fences the late commit/DLQ write, reports pending>0 with a drain_timeout
// reason, and the uncommitted head is redelivered after restart.
func TestShutdownTimeoutFencesCommit(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 64
	cfg.BlockPublish = false
	b, dir := newTestBus(t, cfg)
	logger, _ := testLogger(t)

	mustPublish(t, b, "k", "stuck-head", nil)
	mustPublish(t, b, "k", "behind-1", nil)

	var attempts atomic.Int32
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		a := attempts.Add(1)
		logger.Printf("HANDLE id=%s offset=%d attempt=%d (failing)", m.ID, m.Offset, a)
		// Fail fast enough to be mid-retry when shutdown starts, but not
		// so fast that it DLQs within the window: long backoff.
		<-time.After(80 * time.Millisecond)
		return errors.New("transient-forever")
	}), WithMaxAttempts(100000), WithBackoff(ExponentialBackoff{
		Base: 5 * time.Second, Max: 5 * time.Second,
	}), WithLogger(logger))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool { return attempts.Load() >= 1 }, "first attempt started")

	drain := 120 * time.Millisecond
	start := time.Now()
	rep, err := b.Shutdown(WithDrainTimeout(drain), WithStopGrace(200*time.Millisecond))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// Must return bounded: drain + grace + slack, never wait the 5s backoff.
	if elapsed > drain+time.Second {
		t.Fatalf("shutdown took %s, exceeds bound", elapsed)
	}
	t.Logf("shutdown returned after %s outcome=%s reason=%s", elapsed, rep.Outcome, rep.Reason)
	if rep.Outcome != OutcomeTimedOut {
		t.Fatalf("outcome=%s, want timeout", rep.Outcome)
	}
	if len(rep.Groups) != 1 || rep.Groups[0].GroupID != "g" {
		t.Fatalf("groups=%+v", rep.Groups)
	}
	g := rep.Groups[0]
	if g.Pending != 2 {
		t.Errorf("group pending=%d, want 2 (head uncommitted blocks both)", g.Pending)
	}
	ps := g.Partitions[0]
	if ps.CommittedNext != 0 || ps.LogEndOffset != 2 || ps.Pending != 2 {
		t.Errorf("partition line=%+v, want committed=0 end=2 pending=2", ps)
	}
	if ps.InterruptReason != string(GroupInterruptDrainTimeout) {
		t.Errorf("partition reason=%q, want drain_timeout", ps.InterruptReason)
	}
	logger2 := logger // keep reference
	_ = logger2

	// Restart: the uncommitted head and the message behind it redeliver;
	// nothing was falsely committed.
	b2, err := New(Config{DataDir: dir, Partitions: 1, QueueCapacity: 256, BlockPublish: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b2.Close() })
	var mu sync.Mutex
	got := map[string]int{}
	c2, _ := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		mu.Lock()
		got[m.ID]++
		logger2.Printf("REDELIVER id=%s offset=%d", m.ID, m.Offset)
		mu.Unlock()
		return nil
	}), WithBackoff(fastBackoff()))
	go func() { _ = c2.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got["stuck-head"] == 1 && got["behind-1"] == 1
	}, "both uncommitted messages redelivered once")
	mu.Lock()
	if len(got) != 2 {
		t.Errorf("redelivered set=%v, want exactly 2 ids", got)
	}
	mu.Unlock()
}

// TestShutdownRefusesNewWorkWithDistinctErrors verifies publishes during
// draining match ErrBusShuttingDown (and ErrBusClosed), while publishes
// after completion match only ErrBusClosed; repeat Shutdown is idempotent;
// concurrent shutdown callers all get the same report within the deadline.
func TestShutdownRefusesNewWorkWithDistinctErrors(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 1
	cfg.BlockPublish = true // publisher will block waiting for room
	b, _ := newTestBus(t, cfg)

	// Stall the single consumer so the queue fills and a publisher parks.
	proceed := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		startOnce.Do(func() { close(started) })
		<-proceed
		<-ctx.Done() // stay inside the handler through the drain window
		return ctx.Err()
	}), WithBackoff(fastBackoff()))
	go func() { _ = c.Run(context.Background()) }()

	mustPublish(t, b, "k", "m0", nil)
	<-started

	blockedErr := make(chan error, 1)
	go func() {
		_, _, err := b.Publish("k", "blocked", nil)
		blockedErr <- err
	}()
	time.Sleep(50 * time.Millisecond) // ensure it is parked in WaitRoom

	// Start shutdown concurrently from 3 goroutines. All must return within
	// the deadline and observe the same outcome.
	var wg sync.WaitGroup
	reports := make([]*ShutdownReport, 3)
	errs := make([]error, 3)
	drain := 300 * time.Millisecond
	shutdownStart := time.Now()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := b.Shutdown(WithDrainTimeout(drain), WithStopGrace(500*time.Millisecond))
			reports[i] = r
			errs[i] = e
		}(i)
	}

	// A publish attempted during the drain must be refused. Give shutdown
	// a moment to flip the state, then try from another goroutine.
	time.Sleep(30 * time.Millisecond)
	_, _, drainErr := b.Publish("k", "during-drain", nil)

	wg.Wait()
	close(proceed)
	if time.Since(shutdownStart) > drain+2*time.Second {
		t.Fatalf("concurrent shutdowns did not return within the bound")
	}
	for i := range reports {
		if errs[i] != nil {
			t.Errorf("shutdown[%d] err=%v", i, errs[i])
		}
		if reports[i] == nil || reports[i].Outcome != OutcomeTimedOut {
			t.Fatalf("shutdown[%d] report=%+v", i, reports[i])
		}
		if !reflect.DeepEqual(reports[i], reports[0]) {
			t.Errorf("shutdown[%d] returned a different report than caller 0: %+v vs %+v",
				i, reports[i], reports[0])
		}
	}

	// The blocked publisher was woken by the drain start, not left waiting.
	select {
	case err := <-blockedErr:
		if !errors.Is(err, ErrBusClosed) {
			t.Errorf("blocked publisher err=%v, want wraps ErrBusClosed", err)
		}
		if !errors.Is(err, ErrBusShuttingDown) {
			t.Errorf("blocked publisher err=%v, want ErrBusShuttingDown while draining", err)
		}
		t.Logf("blocked publisher unblocked with: %v", err)
	case <-time.After(testTimeout):
		t.Fatal("blocked publisher never unblocked on shutdown")
	}

	// Distinguish drain-stage refusal from fully-closed refusal.
	if drainErr == nil {
		t.Fatal("publish during drain returned nil")
	}
	if !errors.Is(drainErr, ErrBusShuttingDown) {
		t.Errorf("publish during drain err=%v, want ErrBusShuttingDown", drainErr)
	}
	if !errors.Is(drainErr, ErrBusClosed) {
		t.Errorf("publish during drain should also match ErrBusClosed: %v", drainErr)
	}
	var bce *BusClosedError
	if !errors.As(drainErr, &bce) || bce.Stage != stageDraining {
		t.Errorf("publish during drain error type=%T, want stage=draining", drainErr)
	}

	// After completion: plain closed stage.
	_, _, closedErr := b.Publish("k", "after", nil)
	if !errors.Is(closedErr, ErrBusClosed) {
		t.Errorf("publish after close err=%v", closedErr)
	}
	if errors.Is(closedErr, ErrBusShuttingDown) {
		t.Errorf("publish after close must NOT match shutting-down: %v", closedErr)
	}
	if !errors.As(closedErr, &bce) || bce.Stage != stageClosed {
		t.Errorf("post-close error stage=%v, want closed", bce)
	}

	// Repeat shutdown is safe/idempotent and returns the cached report.
	r2, err2 := b.Shutdown()
	if err2 != nil {
		t.Errorf("repeat shutdown err=%v", err2)
	}
	if !reflect.DeepEqual(r2, reports[0]) {
		t.Errorf("repeat shutdown returned a different report: %+v", r2)
	}
}

// TestShutdownComboStress is the headline combination requested:
// bounded queue full + a blocked producer waiting for capacity +
// consumers mid-retry + shutdown initiated concurrently — repeated over
// many rounds. Each round asserts:
//
//   - Shutdown returns within the configured bound (never hangs on the
//     retry backoff or the blocked producer);
//   - no successful message is confirmed twice (tracked per id);
//   - after reopen, every uncommitted message is redelivered and then
//     successfully processed exactly once; nothing is lost;
//   - final committed offsets only stay or move forward.
func TestShutdownComboStress(t *testing.T) {
	const rounds = 40
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round-%02d", round), func(t *testing.T) {
			runComboRound(t, round)
		})
	}
}

func runComboRound(t *testing.T, round int) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		Partitions:    2,
		QueueCapacity: 4,
		BlockPublish:  true, // 4th+ publisher parks until capacity frees
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	logger, _ := testLogger(t)

	const prefill = 6 // more than capacity(4): first 4 fit (lag-based bound)
	for i := 0; i < prefill; i++ {
		mustPublish(t, b, fmt.Sprintf("k%d", i%2), fmt.Sprintf("r%d-m%02d", round, i), nil)
	}

	// Handlers fail a few times then succeed: round-robin "slow" ids keep
	// the partition in retry/backoff during shutdown.
	var ackMu sync.Mutex
	successCount := map[string]int{}
	var attempts atomic.Int64
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		a := attempts.Add(1)
		// Deterministic-but-interleaved failures: every 3rd invocation fails
		// twice with a long backoff, keeping messages uncommitted at
		// shutdown; everything else succeeds quickly and frees capacity.
		slow := a%7 == 0
		if slow {
			select {
			case <-time.After(60 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
			return errors.New("combo-transient")
		}
		ackMu.Lock()
		successCount[m.ID]++
		cnt := successCount[m.ID]
		ackMu.Unlock()
		logger.Printf("ACK-SUCCESS id=%s partition=%d offset=%d times=%d",
			m.ID, m.Partition, m.Offset, cnt)
		return nil
	}), WithMaxAttempts(100000), WithBackoff(ExponentialBackoff{
		Base: 2 * time.Second, Max: 2 * time.Second,
	}), WithLogger(logger))
	go func() { _ = c.Run(context.Background()) }()

	// Give loops a moment to enter processing/retry and grow lag.
	time.Sleep(20 * time.Millisecond)

	// Start producers that must park against the full queue.
	const blockedN = 3
	var pubWG sync.WaitGroup
	pubResults := make([]error, blockedN)
	for i := 0; i < blockedN; i++ {
		pubWG.Add(1)
		go func(i int) {
			defer pubWG.Done()
			id := fmt.Sprintf("r%d-blocked-%d", round, i)
			_, _, e := b.Publish(fmt.Sprintf("k%d", i%2), id, nil)
			pubResults[i] = e
		}(i)
	}

	// Fire shutdown concurrently from 2 goroutines to stress idempotency.
	drain := 100 * time.Millisecond
	repCh := make(chan *ShutdownReport, 2)
	var shutWG sync.WaitGroup
	startShutdown := make(chan struct{})
	for i := 0; i < 2; i++ {
		shutWG.Add(1)
		go func() {
			defer shutWG.Done()
			<-startShutdown
			r, e := b.Shutdown(WithDrainTimeout(drain), WithStopGrace(400*time.Millisecond))
			if e != nil {
				t.Errorf("shutdown err: %v", e)
			}
			repCh <- r
		}()
	}
	time.Sleep(10 * time.Millisecond) // producers parked, consumers retrying
	t0 := time.Now()
	close(startShutdown)
	shutWG.Wait()
	elapsed := time.Since(t0)
	close(repCh)
	pubWG.Wait()

	// Timing bound: drain + grace + scheduling slack, never the 2s backoff
	// or a deadlocked producer.
	if elapsed > drain+1500*time.Millisecond {
		t.Fatalf("round %d: shutdown took %s, exceeds bound", round, elapsed)
	}
	t.Logf("round=%d shutdown elapsed=%s", round, elapsed)

	// Blocked publishers must all have returned; those refused during the
	// drain report ErrBusShuttingDown; none may have silently succeeded
	// after shutdown started unless they legitimately drained beforehand.
	var parkedAccepted, parkedRefused int
	for i, e := range pubResults {
		switch {
		case e == nil:
			parkedAccepted++
			t.Logf("round=%d blocked[%d] published before fence", round, i)
		case errors.Is(e, ErrBusShuttingDown) || errors.Is(e, ErrBusClosed):
			parkedRefused++
			t.Logf("round=%d blocked[%d] refused: %v", round, i, e)
		default:
			t.Errorf("round=%d blocked[%d] unexpected err=%v", round, i, e)
		}
	}
	if parkedAccepted+parkedRefused != blockedN {
		t.Fatalf("round=%d: %d parked publishers did not return", round, blockedN-parkedAccepted-parkedRefused)
	}

	// Snapshot the shutdown report and verify internal consistency.
	var rep *ShutdownReport
	nrep := 0
	for r := range repCh {
		if rep == nil {
			rep = r
		} else if !reflect.DeepEqual(r, rep) {
			t.Errorf("round=%d concurrent reports differ", round)
		}
		nrep++
	}
	if nrep != 2 || rep == nil {
		t.Fatalf("round=%d expected 2 reports, got %d", round, nrep)
	}
	var reportPending int64
	for _, g := range rep.Groups {
		for _, ps := range g.Partitions {
			reportPending += ps.Pending
			t.Logf("round=%d report group=%s part=%d committed_next=%d end=%d pending=%d newDLQ=%d reason=%s",
				round, g.GroupID, ps.Partition, ps.CommittedNext, ps.LogEndOffset,
				ps.Pending, ps.NewDLQ, ps.InterruptReason)
			if ps.CommittedNext > ps.LogEndOffset {
				t.Errorf("round=%d committed %d > log end %d", round, ps.CommittedNext, ps.LogEndOffset)
			}
		}
	}
	if reportPending == 0 && rep.Outcome != OutcomeDrained {
		t.Errorf("round=%d no pending but outcome=%s", round, rep.Outcome)
	}

	// No successful ack duplicated before shutdown (same id never twice).
	ackMu.Lock()
	for id, cnt := range successCount {
		if cnt > 1 {
			t.Errorf("round=%d id=%s success-acked %d times during run (want <=1)", round, id, cnt)
		}
	}
	ackMu.Unlock()

	// Restart: everything uncommitted is redelivered; combined with earlier
	// successes every originally published message ends up processed
	// exactly once. Replay WALs to know the full surviving id set.
	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b2.Close() })

	// IDs durable in the WALs: prefill always; blocked ones only if they
	// won the race into capacity before the fence.
	wantIDs := map[string]bool{}
	for pi, w := range b2.partitions {
		_ = pi
		for i := 0; i < w.Len(); i++ {
			m, ok := w.Get(int64(i))
			if !ok {
				t.Fatal("missing replayed record")
			}
			wantIDs[m.ID] = true
		}
	}
	var finalMu sync.Mutex
	finalCount := map[string]int{}
	c2, _ := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		finalMu.Lock()
		finalCount[m.ID]++
		logger.Printf("AFTER-RESTART id=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		finalMu.Unlock()
		return nil
	}), WithBackoff(fastBackoff()), WithLogger(logger))
	go func() { _ = c2.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool {
		// A durable message is done if EITHER it was committed before the
		// fence (no redelivery) OR delivered/acked after restart.
		ackMu.Lock()
		finalMu.Lock()
		defer finalMu.Unlock()
		defer ackMu.Unlock()
		for id := range wantIDs {
			if successCount[id] == 0 && finalCount[id] == 0 {
				return false
			}
		}
		return true
	}, fmt.Sprintf("round %d: all durable messages consumed after restart", round))

	// Exactly-once across the whole lifecycle for every surviving message:
	// pre-shutdown success + post-restart delivery == 1 per id.
	ackMu.Lock()
	finalMu.Lock()
	for id := range wantIDs {
		before := successCount[id]
		after := finalCount[id]
		if before+after != 1 {
			t.Errorf("round=%d id=%s lifecycle success count=%d (before=%d after=%d), want exactly 1",
				round, id, before+after, before, after)
		}
	}
	finalMu.Unlock()
	ackMu.Unlock()

	// Offsets after full drain equal each log length and match/advance the
	// shutdown report (never regress).
	for _, g := range rep.Groups {
		for _, ps := range g.Partitions {
			wantEnd := b2.partitions[ps.Partition].Len()
			got := b2.offsets.Get(g.GroupID, ps.Partition)
			if got != int64(wantEnd) {
				t.Errorf("round=%d part=%d final offset=%d want=%d", round, ps.Partition, got, wantEnd)
			}
			if got < ps.CommittedNext {
				t.Errorf("round=%d part=%d offset regressed %d -> %d",
					round, ps.Partition, ps.CommittedNext, got)
			}
		}
	}

	// Blocked publishers whose message never made it to the WAL are not
	// "lost": the caller received an explicit error and must retry.
	_ = b2
}

// TestShutdownHandlerStuckReason verifies a handler that ignores ctx and
// is still executing when the drain window ends: Shutdown returns after at
// most drain+grace, reports handler_stuck, and the handler's late commit is
// fenced (never writes after storage release).
func TestShutdownHandlerStuckReason(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 64
	cfg.BlockPublish = false
	b, _ := newTestBus(t, cfg)
	mustPublish(t, b, "k", "stuck", nil)
	mustPublish(t, b, "k", "queued", nil)

	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		once.Do(func() { close(entered) })
		<-release // deliberately ignore cancellation
		return nil
	}))
	go func() { _ = c.Run(context.Background()) }()
	<-entered

	drain, grace := 80*time.Millisecond, 120*time.Millisecond
	start := time.Now()
	rep, err := b.Shutdown(WithDrainTimeout(drain), WithStopGrace(grace))
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > drain+grace+time.Second {
		t.Fatalf("shutdown stuck: %s", elapsed)
	}
	t.Logf("shutdown returned after %s", elapsed)
	if rep.Outcome != OutcomeTimedOut || rep.Reason != string(GroupInterruptHandlerStuck) {
		t.Fatalf("outcome=%s reason=%s, want timeout/handler_stuck", rep.Outcome, rep.Reason)
	}
	g := rep.Groups[0]
	ps := g.Partitions[0]
	if ps.InterruptReason != string(GroupInterruptHandlerStuck) {
		t.Errorf("partition reason=%q want handler_stuck", ps.InterruptReason)
	}
	if ps.CommittedNext != 0 || ps.Pending != 2 {
		t.Errorf("line=%+v, want committed=0 pending=2", ps)
	}

	// Release the handler AFTER storage is gone: its commit must be fenced
	// and must not panic/write-after-close.
	close(release)
	time.Sleep(50 * time.Millisecond)
	if err := c.Ack(Message{Partition: 0, Offset: 0}); !errors.Is(err, ErrShutdownFenced) {
		t.Errorf("late Ack err=%v, want ErrShutdownFenced", err)
	}
}

// TestShutdownDLQDuringDrain verifies a message that reaches a terminal
// DLQ outcome inside the drain window is persisted, advances the offset,
// is counted as new_dlq in the report, and is not redelivered after
// restart.
func TestShutdownDLQDuringDrain(t *testing.T) {
	cfg := baseCfg()
	cfg.Partitions = 1
	cfg.QueueCapacity = 64
	cfg.BlockPublish = false
	b, dir := newTestBus(t, cfg)
	mustPublish(t, b, "k", "poison", nil)
	mustPublish(t, b, "k", "ok", nil)

	var started atomic.Bool
	c, _ := b.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		started.Store(true)
		if m.ID == "poison" {
			// Hold until shutdown drain begins, then DLQ within the window.
			time.Sleep(100 * time.Millisecond)
			return NonRetryable(errors.New("bad"))
		}
		return nil
	}), WithLogger(nil))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, started.Load, "handler started")

	rep, err := b.Shutdown(WithDrainTimeout(2*time.Second), WithStopGrace(time.Second))
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if rep.Outcome != OutcomeDrained {
		t.Fatalf("outcome=%s reason=%s", rep.Outcome, rep.Reason)
	}
	g := rep.Groups[0]
	if g.NewDLQ != 1 || g.Partitions[0].NewDLQ != 1 {
		t.Errorf("newDLQ group=%d part=%d, want 1", g.NewDLQ, g.Partitions[0].NewDLQ)
	}
	if g.Partitions[0].CommittedNext != 2 {
		t.Errorf("committed=%d, want 2", g.Partitions[0].CommittedNext)
	}
	dlq, _ := b.dlq.Entries()
	if len(dlq) != 1 || dlq[0].Message.ID != "poison" || dlq[0].Reason != ReasonNonRetryable {
		t.Fatalf("dlq=%+v", dlq)
	}

	// Restart: offsets at 2 (nothing redelivered), report + DLQ persisted.
	b2, err := New(Config{DataDir: dir, Partitions: 1, QueueCapacity: 256, BlockPublish: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b2.Close() })
	if got := b2.offsets.Get("g", 0); got != 2 {
		t.Errorf("after restart offset=%d, want 2", got)
	}
	if last := b2.LastShutdownReport(); last == nil || last.Groups[0].NewDLQ != 1 {
		t.Errorf("reloaded report newDLQ mismatch: %+v", last)
	}
	if entries, _ := b2.DLQEntries(); len(entries) != 1 || entries[0].Reason != ReasonNonRetryable {
		t.Errorf("reloaded DLQ=%+v", entries)
	}
	var redispatched atomic.Int32
	c2, _ := b2.Subscribe("g", HandlerFunc(func(ctx context.Context, m Message) error {
		redispatched.Add(1)
		return nil
	}))
	go func() { _ = c2.Run(context.Background()) }()
	time.Sleep(150 * time.Millisecond)
	c2.Stop()
	c2.Wait()
	if redispatched.Load() != 0 {
		t.Errorf("%d redeliveries after in-drain DLQ shutdown", redispatched.Load())
	}
}
