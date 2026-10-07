package bus

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultShutdownTimeout = 5 * time.Second
	defaultStopGrace       = 250 * time.Millisecond
	shutdownFileName       = "shutdown.json"
)

// Shutdown stages reported by BusClosedError.
const (
	stageDraining = "draining"
	stageClosed   = "closed"
)

// ShutdownOutcome classifies how a graceful shutdown ended.
type ShutdownOutcome string

const (
	// OutcomeDrained: every subscribed group finished its backlog within
	// the drain window.
	OutcomeDrained ShutdownOutcome = "drained"
	// OutcomeTimedOut: the drain window elapsed with work outstanding.
	OutcomeTimedOut ShutdownOutcome = "timeout"
)

// Group-level outcomes.
const (
	groupOutcomeDrained     = "drained"
	groupOutcomeInterrupted = "interrupted"
)

// GroupInterruptReason explains why a group did not finish its backlog.
type GroupInterruptReason string

const (
	// GroupDrained: the group committed every stored record.
	GroupDrained GroupInterruptReason = "drained"
	// GroupInterruptDrainTimeout: the bus drain window elapsed while the
	// group was running but behind.
	GroupInterruptDrainTimeout GroupInterruptReason = "drain_timeout"
	// GroupInterruptHandlerStuck: the stop-grace elapsed while a handler
	// was still executing (it is fenced: its result is not committed).
	GroupInterruptHandlerStuck GroupInterruptReason = "handler_stuck"
	// GroupInterruptNotRunning: the group was subscribed but its Run loop
	// never processed the outstanding positions.
	GroupInterruptNotRunning GroupInterruptReason = "not_running"
)

// ShutdownOptions configures one Shutdown call. A zero value uses the bus
// Config defaults.
type ShutdownOptions struct {
	// DrainTimeout bounds how long new work is refused while in-flight
	// messages, retry backoffs and uncommitted records are allowed to
	// finish. Zero falls back to Config.ShutdownTimeout (default 5s).
	DrainTimeout time.Duration
	// StopGrace bounds waiting, after the drain window ends, for handlers
	// currently executing to return; afterwards their result is fenced.
	// Zero falls back to Config.StopGrace (default 250ms).
	StopGrace time.Duration
}

// ShutdownOption mutates ShutdownOptions.
type ShutdownOption func(*ShutdownOptions)

// WithDrainTimeout overrides the drain window for one Shutdown.
func WithDrainTimeout(d time.Duration) ShutdownOption {
	return func(o *ShutdownOptions) { o.DrainTimeout = d }
}

// WithStopGrace overrides the post-drain handler grace for one Shutdown.
func WithStopGrace(d time.Duration) ShutdownOption {
	return func(o *ShutdownOptions) { o.StopGrace = d }
}

// PartitionShutdownState is one per-group/per-partition line of a report.
type PartitionShutdownState struct {
	Partition       int    `json:"partition"`
	CommittedNext   int64  `json:"committed_next"`
	LogEndOffset    int64  `json:"log_end_offset"`
	Pending         int64  `json:"pending"`
	NewDLQ          int    `json:"new_dlq"`
	InterruptReason string `json:"interrupt_reason,omitempty"`
}

// GroupShutdownState aggregates one consumer group in a ShutdownReport.
type GroupShutdownState struct {
	GroupID    string                   `json:"group_id"`
	Outcome    string                   `json:"outcome"`
	Pending    int64                    `json:"pending"`
	NewDLQ     int                      `json:"new_dlq"`
	Reason     string                   `json:"reason,omitempty"`
	Partitions []PartitionShutdownState `json:"partitions"`
}

// ShutdownReport is the explainable result of a graceful shutdown. Every
// position/count is read from already-persisted state after consumers have
// quiesced, so the same report reloaded after restart cannot contradict
// the offset/DLQ stores.
type ShutdownReport struct {
	StartedAt    time.Time            `json:"started_at"`
	FinishedAt   time.Time            `json:"finished_at"`
	DrainTimeout time.Duration        `json:"drain_timeout"`
	Outcome      ShutdownOutcome      `json:"outcome"`
	Reason       string               `json:"reason,omitempty"`
	Groups       []GroupShutdownState `json:"groups"`
}

// Shutdown performs the unified graceful shutdown:
//
//  1. the bus stops accepting new publishes/subscriptions (already-blocked
//     publishers are woken with ErrBusShuttingDown);
//  2. in-flight handlers, retry backoffs and uncommitted messages get up to
//     DrainTimeout to finish; commits that succeed in the window are
//     persisted as usual;
//  3. when the window ends, delivery loops are cancelled; commits/DLQ
//     writes after that are fenced off (ErrShutdownFenced) — uncommitted
//     messages are redelivered on the next start, committed positions only
//     stay or advance;
//  4. storage is released and an explainable ShutdownReport is returned
//     and persisted (see LastShutdownReport).
//
// Shutdown is safe to call concurrently and repeatedly: every caller after
// the first waits for completion and receives the same report.
func (b *Bus) Shutdown(opts ...ShutdownOption) (*ShutdownReport, error) {
	var o ShutdownOptions
	for _, opt := range opts {
		opt(&o)
	}
	drain := o.DrainTimeout
	if drain <= 0 {
		drain = b.cfg.ShutdownTimeout
	}
	if drain <= 0 {
		drain = defaultShutdownTimeout
	}
	grace := o.StopGrace
	if grace <= 0 {
		grace = b.cfg.StopGrace
	}
	if grace <= 0 {
		grace = defaultStopGrace
	}
	return b.shutdown(drain, grace)
}

// shutdown is the single orchestration entry used by both Shutdown and the
// legacy Close (drain=0, grace=0).
func (b *Bus) shutdown(drain, grace time.Duration) (*ShutdownReport, error) {
	if !b.state.CompareAndSwap(stateOpen, stateDraining) {
		// Another caller owns the transition: wait it out and return the
		// same result. Idempotent, never an error for repeat callers.
		<-b.closeCh
		return b.reportSnapshot(), nil
	}

	startedAt := time.Now()
	dlqAtStart := b.dlqCounts()

	// 1. Refuse new work and release publishers parked in WaitRoom.
	close(b.drainCh)
	for _, p := range b.partitions {
		p.seal() // reject appends; consumers keep draining the backlog
	}
	b.slogf("shutdown: draining started drain_timeout=%s stop_grace=%d groups=%d",
		drain, grace, len(b.groupSnapshot()))

	// 2. Drain window: finish as much outstanding work as possible.
	timedOut := false
	if drain <= 0 {
		b.closeDeadline()
		timedOut = true
	} else {
		timer := time.NewTimer(drain)
		for !b.allSettled() {
			select {
			case <-timer.C:
				timedOut = true
			case <-b.drainSignal():
			}
			if timedOut {
				break
			}
		}
		timer.Stop()
		if timedOut {
			b.closeDeadline()
		}
	}

	// 3. Window over (or everything settled): cancel every delivery loop.
	// Closing the deadline is what cancels runCtx; requestStop is belt-and-
	// braces for loops parked elsewhere.
	b.closeDeadline()
	for _, c := range b.groupSnapshot() {
		c.requestStop()
	}

	// 4. Give handlers executing right at the boundary a bounded grace to
	// return; their subsequent commit is fenced regardless.
	allExited := true
	if grace > 0 {
		for _, c := range b.groupSnapshot() {
			if !c.waitDone(grace) {
				allExited = false
			}
		}
	} else {
		for _, c := range b.groupSnapshot() {
			if c.isRunning() {
				allExited = false
			}
		}
	}

	finishedAt := time.Now()
	report := b.buildReport(startedAt, finishedAt, drain, timedOut, allExited, dlqAtStart)

	// 5. Persist the report BEFORE releasing storage, then close. From here
	// on no consumer can write: the fence rejected all post-window commits,
	// and loops have exited (or were abandoned while stuck inside a
	// non-cancellable handler, in which case the fence still blocks them).
	if err := b.persistReport(report); err != nil {
		b.slogf("shutdown: report persist failed: %v", err)
	}
	for _, p := range b.partitions {
		p.close()
	}
	b.mu.Lock()
	b.report = report
	b.mu.Unlock()
	b.state.Store(stateClosed)
	closeErr := b.closeStorage()
	close(b.closeCh)

	b.slogf("shutdown: finished outcome=%s groups=%d", report.Outcome, len(report.Groups))
	return report, closeErr
}

// closeDeadline closes the deadline channel exactly once.
func (b *Bus) closeDeadline() {
	b.deadlineOnce.Do(func() { close(b.deadlineCh) })
}

// groupSnapshot returns the currently subscribed consumers without
// holding bus.mu during later offset/partition reads.
func (b *Bus) groupSnapshot() []*Consumer {
	b.mu.RLock()
	out := make([]*Consumer, 0, len(b.groups))
	for _, c := range b.groups {
		out = append(out, c)
	}
	b.mu.RUnlock()
	return out
}

// allSettled reports whether shutdown can finish the drain phase early.
// A running group is settled only when every partition's committed
// watermark equals the log length; a group whose loop already exited is
// considered settled (nothing more can drain from it — its remaining
// backlog shows up as pending in the report).
func (b *Bus) allSettled() bool {
	for _, c := range b.groupSnapshot() {
		if c.isRunning() {
			for _, p := range b.partitions {
				if b.offsets.Get(c.groupID, p.partition) != int64(p.Len()) {
					return false
				}
			}
		}
	}
	return true
}

// dlqCounts returns {group: {partition: count}} from the persisted DLQ.
func (b *Bus) dlqCounts() map[string]map[int]int {
	out := map[string]map[int]int{}
	entries, err := b.dlq.Entries()
	if err != nil {
		return out
	}
	for _, e := range entries {
		m, ok := out[e.GroupID]
		if !ok {
			m = map[int]int{}
			out[e.GroupID] = m
		}
		m[e.Message.Partition]++
	}
	return out
}

func dlqDelta(start map[string]map[int]int, now map[string]map[int]int, group string, partition int) int {
	return now[group][partition] - start[group][partition]
}

// buildReport reads only quiesced, persisted state: offsets from the
// OffsetStore, log ends from in-memory partitions, DLQ deltas from the
// DLQ file. The numbers therefore match what a restart will observe.
func (b *Bus) buildReport(
	startedAt, finishedAt time.Time,
	drain time.Duration,
	timedOut, allExited bool,
	dlqAtStart map[string]map[int]int,
) *ShutdownReport {
	dlqNow := b.dlqCounts()
	r := &ShutdownReport{
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
		DrainTimeout: drain,
		Outcome:      OutcomeDrained,
		Groups:       []GroupShutdownState{},
	}

	for _, c := range b.groupSnapshot() {
		gs := GroupShutdownState{
			GroupID:    c.groupID,
			Outcome:    groupOutcomeDrained,
			Partitions: []PartitionShutdownState{},
		}
		worst := GroupDrained
		for _, p := range b.partitions {
			committed := b.offsets.Get(c.groupID, p.partition)
			end := int64(p.Len())
			pending := end - committed
			if pending < 0 {
				pending = 0
			}
			ps := PartitionShutdownState{
				Partition:     p.partition,
				CommittedNext: committed,
				LogEndOffset:  end,
				Pending:       pending,
				NewDLQ:        dlqDelta(dlqAtStart, dlqNow, c.groupID, p.partition),
			}
			if pending > 0 {
				reason := b.partitionInterruptReason(c, p.partition)
				ps.InterruptReason = string(reason)
				gs.Outcome = groupOutcomeInterrupted
				worst = worseReason(worst, reason)
			}
			gs.Pending += pending
			gs.NewDLQ += ps.NewDLQ
			gs.Partitions = append(gs.Partitions, ps)
		}
		if gs.Outcome == groupOutcomeInterrupted {
			gs.Reason = string(worst)
		}
		r.Groups = append(r.Groups, gs)
	}

	anyPending := false
	for _, g := range r.Groups {
		if g.Pending > 0 {
			anyPending = true
			break
		}
	}
	switch {
	case anyPending || timedOut:
		r.Outcome = OutcomeTimedOut
		r.Reason = string(GroupInterruptDrainTimeout)
		if !allExited {
			r.Reason = string(GroupInterruptHandlerStuck)
		}
	default:
		r.Outcome = OutcomeDrained
	}
	return r
}

// partitionInterruptReason classifies why one partition has pending work.
func (b *Bus) partitionInterruptReason(c *Consumer, partition int) GroupInterruptReason {
	switch {
	case c.inflight(partition):
		return GroupInterruptHandlerStuck
	case !c.ranEver():
		return GroupInterruptNotRunning
	default:
		return GroupInterruptDrainTimeout
	}
}

// worseReason orders interrupt reasons by diagnostic severity.
func worseReason(a, b GroupInterruptReason) GroupInterruptReason {
	rank := func(r GroupInterruptReason) int {
		switch r {
		case GroupInterruptHandlerStuck:
			return 3
		case GroupInterruptNotRunning:
			return 2
		case GroupInterruptDrainTimeout:
			return 1
		default:
			return 0
		}
	}
	if rank(b) > rank(a) {
		return b
	}
	return a
}

// reportPath is where the last shutdown report is persisted.
func (b *Bus) reportPath() string { return filepath.Join(b.cfg.DataDir, shutdownFileName) }

// persistReport atomically writes the report (tmp + fsync + rename).
func (b *Bus) persistReport(r *ShutdownReport) error {
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	path := b.reportPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// loadLastReport reads a persisted shutdown report; a missing/corrupt file
// yields nil (never blocks startup).
func (b *Bus) loadLastReport() *ShutdownReport {
	raw, err := os.ReadFile(b.reportPath())
	if err != nil {
		return nil
	}
	var r ShutdownReport
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil
	}
	return &r
}

func (b *Bus) reportSnapshot() *ShutdownReport {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return cloneReport(b.report)
}

// LastShutdownReport returns a copy of the most recent shutdown report.
// After a restart this is the report persisted by the previous run, so
// post-restart queries stay consistent with stored offsets/DLQ.
func (b *Bus) LastShutdownReport() *ShutdownReport {
	return b.reportSnapshot()
}

func cloneReport(r *ShutdownReport) *ShutdownReport {
	if r == nil {
		return nil
	}
	cp := *r
	if r.Groups != nil {
		cp.Groups = make([]GroupShutdownState, len(r.Groups))
		for i, g := range r.Groups {
			cp.Groups[i] = g
			if g.Partitions != nil {
				cp.Groups[i].Partitions = append([]PartitionShutdownState(nil), g.Partitions...)
			}
		}
	}
	return &cp
}

func (b *Bus) slogf(format string, args ...any) {
	if b.cfg.Logger != nil {
		b.cfg.Logger.Printf(format, args...)
	}
}
