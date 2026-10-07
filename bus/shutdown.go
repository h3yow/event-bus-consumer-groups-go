package bus

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ShutdownOutcome distinguishes how a graceful shutdown ended.
type ShutdownOutcome string

const (
	// OutcomeDrained: every subscribed group confirmed every published
	// message within the drain window.
	OutcomeDrained ShutdownOutcome = "drained"
	// OutcomeTimeout: the drain window (or caller context) expired with
	// messages still unconfirmed; those will be redelivered after restart.
	OutcomeTimeout ShutdownOutcome = "timeout"
)

// PartitionShutdown reports one partition's state at shutdown: the
// persisted committed "next offset" and how many records were not yet
// confirmed by the group.
type PartitionShutdown struct {
	Partition     int   `json:"partition"`
	CommittedNext int64 `json:"committed_next"`
	Pending       int   `json:"pending"`
}

// GroupShutdown reports one consumer group's shutdown state across all
// partitions plus its dead-letter count.
type GroupShutdown struct {
	GroupID    string             `json:"group_id"`
	Partitions []PartitionShutdown `json:"partitions"`
	DLQCount   int                `json:"dlq_count"`
}

// ShutdownReport is the explainable result of Bus.Shutdown. It is computed
// from the persisted offset store, WAL lengths and DLQ, persisted to
// DataDir/shutdown-report.json, and returned identically to every
// concurrent Shutdown caller (idempotent).
type ShutdownReport struct {
	Outcome      ShutdownOutcome `json:"outcome"`
	Reason       string          `json:"reason"`
	DrainTimeout time.Duration   `json:"drain_timeout"`
	Elapsed      time.Duration   `json:"elapsed"`
	Groups       []GroupShutdown `json:"groups"`
}

// shutdownReportFile is the DataDir file the report is persisted to.
const shutdownReportFile = "shutdown-report.json"

// Shutdown performs a unified graceful shutdown:
//  1. stop accepting new publishes and subscriptions (blocked publishers
//     wake with ErrBusDraining; new calls are rejected with ErrBusDraining);
//  2. wait up to Config.DrainTimeout for in-flight, backing-off and
//     unacknowledged messages to reach a terminal state and persist;
//  3. stop all consumer groups and close storage.
//
// Committed offsets only ever advance; unconfirmed messages are
// redelivered on the next start, and a message can never be
// success-committed twice within a group. Concurrent and repeated calls
// are safe and idempotent: every caller observes the same report. The
// call always returns within DrainTimeout plus a small goroutine-exit
// grace, even if a handler is stuck.
func (b *Bus) Shutdown(ctx context.Context) (ShutdownReport, error) {
	b.mu.Lock()
	if b.report != nil {
		// Shutdown already completed: idempotent replay of the same report.
		r := *b.report
		b.mu.Unlock()
		return r, nil
	}
	if b.draining {
		// A concurrent Shutdown is in progress: wait for its result.
		ch := b.shutdownCh
		b.mu.Unlock()
		select {
		case <-ch:
			b.mu.RLock()
			r := *b.report
			b.mu.RUnlock()
			return r, nil
		case <-ctx.Done():
			return ShutdownReport{}, ctx.Err()
		}
	}
	if b.closed {
		// Close() was called directly without Shutdown.
		b.mu.Unlock()
		return ShutdownReport{}, ErrBusClosed
	}
	b.draining = true
	b.mu.Unlock()

	report := b.runShutdown(ctx)

	b.mu.Lock()
	b.report = &report
	close(b.shutdownCh)
	b.mu.Unlock()
	return report, nil
}

// runShutdown executes the drain/stop/close phases. Called exactly once.
func (b *Bus) runShutdown(ctx context.Context) ShutdownReport {
	start := time.Now()

	// Phase 1: seal partitions. Blocked publishers wake with
	// ErrBusDraining; consumers keep reading so in-flight work drains.
	for _, p := range b.partitions {
		p.seal()
	}

	// Phase 2: wait for quiescence within the drain window.
	deadline := start.Add(b.cfg.DrainTimeout)
	outcome, reason := b.waitDrain(ctx, deadline)

	// Phase 3: stop consumers, bounded so a stuck handler cannot block
	// shutdown forever.
	b.mu.RLock()
	groups := make([]*Consumer, 0, len(b.groups))
	for _, c := range b.groups {
		groups = append(groups, c)
	}
	b.mu.RUnlock()
	for _, c := range groups {
		c.Stop()
	}
	exitGrace := time.Until(deadline)
	if exitGrace < 200*time.Millisecond {
		exitGrace = 200 * time.Millisecond
	}
	exitTimer := time.NewTimer(exitGrace)
	for _, c := range groups {
		if !c.started.Load() {
			// Run was never called: no goroutines to wait for.
			continue
		}
		select {
		case <-c.doneCh:
		case <-exitTimer.C:
			outcome = OutcomeTimeout
			reason += "; consumer goroutines still exiting at close (a handler did not return in time)"
		}
	}
	exitTimer.Stop()

	// Phase 4: snapshot the report from persisted state, persist it, then
	// close. Offsets only advance, so the report never claims more than
	// what is durable.
	report := ShutdownReport{
		Outcome:      outcome,
		Reason:       reason,
		DrainTimeout: b.cfg.DrainTimeout,
		Elapsed:      time.Since(start),
		Groups:       b.snapshotGroups(),
	}
	if err := b.persistReport(report); err != nil {
		report.Reason += "; WARNING: report persistence failed: " + err.Error()
	}

	// Mark closed and wake everyone, but do not hold b.mu while taking
	// partition locks: TryAppend holds p.mu and then acquires b.mu.RLock
	// (via minCommitted), so the reverse order could deadlock.
	b.mu.Lock()
	b.closed = true
	close(b.closeCh)
	b.mu.Unlock()
	for _, p := range b.partitions {
		p.close()
	}
	_ = b.closeStorage()
	return report
}

// waitDrain polls for quiescence until all groups confirmed all records,
// the deadline passes, the caller context ends, or the bus is closed
// concurrently. Polling is cheap (in-memory watermarks) and bounded.
func (b *Bus) waitDrain(ctx context.Context, deadline time.Time) (ShutdownOutcome, string) {
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()
	for {
		if b.quiescent() {
			return OutcomeDrained, "all consumer groups drained within the drain window"
		}
		b.mu.RLock()
		closed := b.closed
		b.mu.RUnlock()
		if closed {
			return OutcomeTimeout, "bus closed during drain"
		}
		select {
		case <-ctx.Done():
			return OutcomeTimeout, "shutdown context canceled: " + ctx.Err().Error()
		case <-time.After(time.Until(deadline)):
			return OutcomeTimeout, "drain timeout exceeded; unconfirmed messages will be redelivered on next start"
		case <-t.C:
		}
	}
}

// quiescent reports whether every subscribed group has confirmed every
// record in every partition.
func (b *Bus) quiescent() bool {
	b.mu.RLock()
	groups := make([]string, 0, len(b.groups))
	for id := range b.groups {
		groups = append(groups, id)
	}
	b.mu.RUnlock()
	for _, g := range groups {
		for _, p := range b.partitions {
			if b.offsets.Get(g, p.partition) < int64(p.Len()) {
				return false
			}
		}
	}
	return true
}

// snapshotGroups builds the per-group/per-partition report from the
// persisted offset store, WAL lengths and DLQ.
func (b *Bus) snapshotGroups() []GroupShutdown {
	dlqCount := map[string]int{}
	if entries, err := b.dlq.Entries(); err == nil {
		for _, e := range entries {
			dlqCount[e.GroupID]++
		}
	}
	b.mu.RLock()
	ids := make([]string, 0, len(b.groups))
	for id := range b.groups {
		ids = append(ids, id)
	}
	b.mu.RUnlock()
	sort.Strings(ids)

	out := make([]GroupShutdown, 0, len(ids))
	for _, id := range ids {
		gs := GroupShutdown{GroupID: id, DLQCount: dlqCount[id]}
		for _, p := range b.partitions {
			committed := b.offsets.Get(id, p.partition)
			pending := int64(p.Len()) - committed
			if pending < 0 {
				pending = 0
			}
			gs.Partitions = append(gs.Partitions, PartitionShutdown{
				Partition:     p.partition,
				CommittedNext: committed,
				Pending:       int(pending),
			})
		}
		out = append(out, gs)
	}
	return out
}

// persistReport writes the report atomically (tmp + rename) next to the
// other DataDir state, so it stays consistent with offsets.json.
func (b *Bus) persistReport(r ShutdownReport) error {
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(b.cfg.DataDir, shutdownReportFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LastShutdown returns the report of the most recent graceful shutdown
// persisted in DataDir, or false if none exists (e.g. the bus was never
// shut down gracefully). A report loaded after a restart is consistent
// with the persisted offsets: committed positions only advance.
func (b *Bus) LastShutdown() (ShutdownReport, bool) {
	b.mu.RLock()
	if b.report != nil {
		r := *b.report
		b.mu.RUnlock()
		return r, true
	}
	b.mu.RUnlock()
	raw, err := os.ReadFile(filepath.Join(b.cfg.DataDir, shutdownReportFile))
	if err != nil {
		return ShutdownReport{}, false
	}
	var r ShutdownReport
	if err := json.Unmarshal(raw, &r); err != nil {
		return ShutdownReport{}, false
	}
	return r, true
}
