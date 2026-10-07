package bus

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Handler processes one message. Returning nil marks success; an error
// triggers retry unless wrapped with NonRetryable.
type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, msg Message) error

func (f HandlerFunc) Handle(ctx context.Context, msg Message) error { return f(ctx, msg) }

// ConsumerConfig configures a consumer group.
type ConsumerConfig struct {
	// MaxAttempts includes the first attempt; MaxAttempts=3 means up to
	// 2 retries. Default 3.
	MaxAttempts int
	// Backoff computes the delay between attempts. Default DefaultBackoff.
	Backoff BackoffPolicy
	// Logger records deliveries, retries, commits and dead-letters.
	// Defaults to the standard package logger.
	Logger *log.Logger
}

// ConsumerOption mutates a ConsumerConfig.
type ConsumerOption func(*ConsumerConfig)

// WithMaxAttempts sets the retry cap (including the first attempt).
func WithMaxAttempts(n int) ConsumerOption {
	return func(c *ConsumerConfig) { c.MaxAttempts = n }
}

// WithBackoff sets the retry backoff policy.
func WithBackoff(p BackoffPolicy) ConsumerOption {
	return func(c *ConsumerConfig) { c.Backoff = p }
}

// WithLogger sets the consumer's logger.
func WithLogger(l *log.Logger) ConsumerOption {
	return func(c *ConsumerConfig) { c.Logger = l }
}

// Consumer is one subscribed consumer group. Within a partition messages
// are delivered strictly in offset order (the head must be committed
// before the next one is processed); different partitions are processed
// concurrently by independent goroutines.
type Consumer struct {
	bus     *Bus
	groupID string
	handler Handler
	cfg     ConsumerConfig

	// ackMu serializes commits across all partition goroutines. The
	// offset store is itself thread-safe; this additionally guarantees
	// that concurrent acks are validated against a single ordering.
	ackMu sync.Mutex
	// watermarks holds the last committed "next offset" per partition,
	// mirroring the OffsetStore for in-process duplicate detection.
	watermarks map[int]int64

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// running is 1 from the first Run until all partition loops exit. It
	// lets Shutdown distinguish "group never started / already stopped"
	// from a group whose handler is still executing.
	running atomic.Bool
	// ran is set true once Run has ever been entered (never reset), so the
	// shutdown report can label a never-run group even long after it stops.
	ran atomic.Bool
	// inFlight marks partitions currently inside Handler.Handle (set right
	// before the call, cleared right after). Used by the shutdown report
	// to label a group whose handler did not return within the grace.
	inFlight sync.Map // partition int -> *atomic.Bool
}

// Run starts one delivery loop per partition and blocks until Stop, ctx
// cancellation or the bus shutdown drain-window deadline.
func (c *Consumer) Run(ctx context.Context) error {
	c.running.Store(true)
	c.ran.Store(true)
	defer c.running.Store(false)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-c.bus.deadlineCh:
			// Drain window over: stop taking/finishing work. The commit
			// fence prevents any storage write after this point.
			cancel()
		case <-runCtx.Done():
		}
	}()

	var wg sync.WaitGroup
	for _, p := range c.bus.partitions {
		wg.Add(1)
		go func(p *PartitionLog) {
			defer wg.Done()
			c.runPartition(runCtx, p)
		}(p)
	}
	wg.Wait()
	close(c.doneCh)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return runCtx.Err()
}

// runPartition is the ordered feeder for one partition.
func (c *Consumer) runPartition(ctx context.Context, p *PartitionLog) {
	// Recovery point: the persisted "next offset". A restarted process
	// resumes exactly here, never re-delivering committed messages and
	// never skipping uncommitted ones.
	offset := c.bus.offsets.Get(c.groupID, p.partition)
	c.ackMu.Lock()
	c.watermarks[p.partition] = offset
	c.ackMu.Unlock()
	c.logf("group=%s partition=%d resume at offset=%d", c.groupID, p.partition, offset)

	for {
		if ctx.Err() != nil {
			return
		}
		// Park until the next record exists; wake-up is loss-free.
		if err := p.WaitOffset(ctx, offset); err != nil {
			return
		}
		msg, ok := p.Get(offset)
		if !ok {
			continue
		}

		terminal := c.deliverWithRetry(ctx, msg)
		if !terminal {
			// ctx cancelled during retry: stop without committing so
			// restart re-delivers the uncommitted message.
			return
		}

		// Success or DLQ: advance the partition order. The commit itself
		// is done inside deliverWithRetry exactly once; here we only move
		// the local cursor.
		offset++
	}
}

// deliverWithRetry processes one message, retrying with backoff. On a
// terminal outcome (success or dead-letter) the offset is committed once
// and true is returned. False means the context ended mid-retry.
func (c *Consumer) deliverWithRetry(ctx context.Context, msg Message) bool {
	maxAttempts := c.cfg.MaxAttempts
	backoff := c.cfg.Backoff
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		msg.Attempt = attempt
		// The drain window may have ended during the retry backoff: do not
		// start another attempt, and never touch storage afterwards.
		if c.bus.fenced() {
			return false
		}
		c.markInflight(msg.Partition, true)
		err := c.handler.Handle(ctx, msg)
		c.markInflight(msg.Partition, false)
		// If the drain window ended while the handler ran, the result must
		// never be committed: after shutdown, storage is being released.
		// The message stays uncommitted and is redelivered next start.
		if c.bus.fenced() {
			c.logf("group=%s id=%s partition=%d offset=%d FENCED after attempt=%d: result discarded, redelivery on restart",
				c.groupID, msg.ID, msg.Partition, msg.Offset, attempt)
			return false
		}
		if err == nil {
			if commitErr := c.commitOnce(msg); commitErr != nil {
				// Commit failure is fatal for the process: the handler
				// already succeeded, so we must not re-run it. Surface
				// the error and stop; the message is still uncommitted,
				// so after restart it is delivered once more (at-least-
				// once; the handler is expected to be idempotent).
				c.logf("group=%s id=%s partition=%d offset=%d COMMIT FAILED: %v",
					c.groupID, msg.ID, msg.Partition, msg.Offset, commitErr)
				return false
			}
			c.logf("group=%s id=%s partition=%d offset=%d attempt=%d ACK committed_next=%d",
				c.groupID, msg.ID, msg.Partition, msg.Offset, attempt, msg.Offset+1)
			return true
		}
		lastErr = err

		var nre *NonRetryableError
		if errors.As(err, &nre) {
			c.deadLetter(msg, ReasonNonRetryable, err, attempt)
			if !c.commitDLQ(msg, ReasonNonRetryable, attempt) {
				return false
			}
			return true
		}

		c.logf("group=%s id=%s partition=%d offset=%d attempt=%d failed: %v",
			c.groupID, msg.ID, msg.Partition, msg.Offset, attempt, err)

		if attempt >= maxAttempts {
			c.deadLetter(msg, ReasonMaxRetriesExceeded, err, attempt)
			if !c.commitDLQ(msg, ReasonMaxRetriesExceeded, attempt) {
				return false
			}
			return true
		}

		delay := backoff.Delay(attempt)
		c.logf("group=%s id=%s partition=%d offset=%d retry in %s (attempt %d/%d)",
			c.groupID, msg.ID, msg.Partition, msg.Offset, delay, attempt+1, maxAttempts)
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-c.bus.deadlineCh:
			t.Stop()
			return false
		case <-t.C:
		}
	}
	_ = lastErr
	return true
}

// commitOnce validates and persists the commit for msg. Ordering and
// duplicates are rejected explicitly rather than silently accepted.
func (c *Consumer) commitOnce(msg Message) error {
	// Hard fence: once the drain window is over (plus grace), no commit is
	// accepted, so storage can never be written after release.
	if c.bus.fenced() {
		return ErrShutdownFenced
	}
	c.ackMu.Lock()
	defer c.ackMu.Unlock()

	cur := c.watermarks[msg.Partition]
	want := msg.Offset + 1
	switch {
	case want <= cur:
		// Same message acked twice (or stale ack).
		return ErrDuplicateAck
	case want > cur+1:
		// Would skip messages that were never committed.
		return ErrOutOfOrderAck
	}
	if err := c.bus.offsets.Commit(c.groupID, msg.Partition, want); err != nil {
		return err
	}
	c.watermarks[msg.Partition] = want

	// Advancing any watermark may free capacity in the bounded partition:
	// wake a publisher blocked in WaitRoom.
	c.bus.partitions[msg.Partition].Notify()
	// Also wake a graceful shutdown waiting for full drain.
	c.bus.notifyDrain()
	return nil
}

func (c *Consumer) deadLetter(msg Message, reason RejectReason, cause error, attempt int) {
	if c.bus.fenced() {
		// Shutdown over: never touch DLQ storage; the head message will be
		// retried/DLQ'd again on the next start.
		return
	}
	entry := DLQEntry{
		GroupID:  c.groupID,
		Message:  msg,
		Reason:   reason,
		LastErr:  cause.Error(),
		Attempts: attempt,
	}
	if err := c.bus.dlq.Append(entry); err != nil {
		c.logf("group=%s id=%s DLQ persist failed: %v", c.groupID, msg.ID, err)
	}
}

// commitDLQ advances the offset after a terminal DLQ outcome. A commit
// failure is not swallowed: the loop stops so restart re-runs the DLQ
// append (idempotency of processing is the handler's contract) and the
// offset is never silently skipped.
func (c *Consumer) commitDLQ(msg Message, reason RejectReason, attempt int) bool {
	if err := c.commitOnce(msg); err != nil {
		c.logf("group=%s id=%s partition=%d offset=%d DLQ COMMIT FAILED (%s): %v",
			c.groupID, msg.ID, msg.Partition, msg.Offset, reason, err)
		return false
	}
	c.logf("group=%s id=%s partition=%d offset=%d DLQ reason=%s attempts=%d committed_next=%d",
		c.groupID, msg.ID, msg.Partition, msg.Offset, reason, attempt, msg.Offset+1)
	return true
}

// Ack explicitly confirms a message. It is optional: the built-in
// run-loop commits automatically after a successful Handle. It is
// exported for handlers that take control of confirmation, and applies
// the same strict ordering/duplicate validation.
func (c *Consumer) Ack(msg Message) error {
	return c.commitOnce(msg)
}

// Stop signals the delivery loops to stop. In-flight retry waits are
// interrupted; uncommitted messages are redelivered on the next Run.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

// Wait blocks until all delivery loops have exited after Stop.
func (c *Consumer) Wait() { <-c.doneCh }

// DLQEntries returns the dead letters recorded for this group.
func (c *Consumer) DLQEntries() ([]DLQEntry, error) {
	all, err := c.bus.dlq.Entries()
	if err != nil {
		return nil, err
	}
	var out []DLQEntry
	for _, e := range all {
		if e.GroupID == c.groupID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (c *Consumer) logf(format string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf(format, args...)
	}
}

// markInflight flags (or clears) that a partition is inside Handle.
func (c *Consumer) markInflight(partition int, active bool) {
	v, _ := c.inFlight.LoadOrStore(partition, &atomic.Bool{})
	v.(*atomic.Bool).Store(active)
}

// inflight reports whether the group has a handler executing on partition
// (-1 = any partition).
func (c *Consumer) inflight(partition int) bool {
	found := false
	c.inFlight.Range(func(k, v any) bool {
		if partition < 0 || k.(int) == partition {
			if v.(*atomic.Bool).Load() {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// isRunning reports whether a Run loop is (or was recently) active.
func (c *Consumer) isRunning() bool { return c.running.Load() }

// ranEver reports whether Run was invoked at least once on this consumer.
func (c *Consumer) ranEver() bool { return c.ran.Load() }

// requestStop is the idempotent stop used by the bus shutdown.
func (c *Consumer) requestStop() { c.stopOnce.Do(func() { close(c.stopCh) }) }

// waitDone blocks until all delivery loops have exited. It returns true
// immediately when the consumer never ran (doneCh is only closed by Run).
func (c *Consumer) waitDone(timeout time.Duration) bool {
	if !c.isRunning() {
		return true
	}
	if timeout <= 0 {
		<-c.doneCh
		return true
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-c.doneCh:
		return true
	case <-t.C:
		return false
	}
}
