package bus

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Bus lifecycle states.
const (
	stateOpen     uint32 = iota // accepting publishes and subscriptions
	stateDraining               // Shutdown started: new work refused, drain in progress
	stateClosed                 // shutdown finished: storage released
)

// Config holds bus-level configuration.
type Config struct {
	// DataDir is where WALs, offsets and DLQ are persisted.
	DataDir string
	// Partitions is the number of partitions (>=1); default 4.
	Partitions int
	// QueueCapacity bounds per-partition lag: records published but not
	// yet committed by the slowest subscribed group. 0 disables the bound.
	QueueCapacity int
	// BlockPublish: true blocks Publish until capacity is available
	// (up to PublishTimeout, zero = forever); false rejects immediately
	// with ErrQueueFull.
	BlockPublish bool
	// PublishTimeout bounds blocking publishes; zero means wait forever.
	PublishTimeout time.Duration
	// ShutdownTimeout is the default drain window for Shutdown: time given
	// to in-flight handlers, retry backoffs and uncommitted messages to
	// finish. Zero defaults to 5s.
	ShutdownTimeout time.Duration
	// StopGrace bounds how long Shutdown waits, after the drain window,
	// for a handler currently executing to return before fencing its
	// commit. Zero defaults to 250ms.
	StopGrace time.Duration
	// Logger, when set, receives shutdown lifecycle log lines.
	Logger *log.Logger
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataDir:       "busdata",
		Partitions:    4,
		QueueCapacity: 1024,
		BlockPublish:  true,
	}
}

// Bus is the local event-driven message bus.
type Bus struct {
	mu         sync.RWMutex
	cfg        Config
	partitions []*PartitionLog
	wals       []*WAL
	groups     map[string]*Consumer
	offsets    *OffsetStore
	dlq        *DLQ

	// Lifecycle state machine: stateOpen -> stateDraining -> stateClosed.
	state        atomic.Uint32
	drainCh      chan struct{}                 // closed once when draining starts
	deadlineCh   chan struct{}                 // closed once when the drain window ends
	closeCh      chan struct{}                 // closed once after storage is released
	drainNotify  atomic.Pointer[chan struct{}] // broadcast: swapped on every commit during drain
	deadlineOnce sync.Once
	report       *ShutdownReport // populated before closeCh is closed

	rrCounter atomic.Uint64
}

// New creates and recovers a Bus from DataDir. WALs and committed offsets
// are replayed, so published messages and confirmed positions survive
// process restarts.
func New(cfg Config) (*Bus, error) {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 4
	}
	if err := mkdir(cfg.DataDir); err != nil {
		return nil, err
	}
	offsets, err := OpenOffsetStore(filepath.Join(cfg.DataDir, "offsets.json"))
	if err != nil {
		return nil, err
	}
	dlq, err := OpenDLQ(filepath.Join(cfg.DataDir, "dlq.log"))
	if err != nil {
		_ = offsets.Close()
		return nil, err
	}
	b := &Bus{
		cfg:        cfg,
		groups:     map[string]*Consumer{},
		offsets:    offsets,
		dlq:        dlq,
		drainCh:    make(chan struct{}),
		deadlineCh: make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
	{
		ch := make(chan struct{})
		b.drainNotify.Store(&ch)
	}
	for i := 0; i < cfg.Partitions; i++ {
		w, replay, err := OpenWAL(cfg.DataDir, i)
		if err != nil {
			_ = b.closeStorage()
			return nil, err
		}
		idx := i
		p := newPartitionLog(i, cfg.QueueCapacity, w, replay, func() int64 {
			return b.minCommitted(idx)
		})
		b.partitions = append(b.partitions, p)
		b.wals = append(b.wals, w)
	}
	// Surface the previous run's shutdown report (if any) so post-restart
	// queries match the persisted offsets/DLQ.
	b.report = b.loadLastReport()
	return b, nil
}

// minCommitted returns the smallest committed "next offset" across all
// subscribed groups for a partition — the slowest group's watermark.
func (b *Bus) minCommitted(partition int) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.groups) == 0 {
		// With no group subscribed nothing can lag; don't apply backpressure.
		// A huge watermark makes lag (len - min) non-positive. Must not call
		// PartitionLog.Len here: the caller may hold the partition lock.
		return 1 << 62
	}
	var min int64 = -1
	for _, c := range b.groups {
		v := b.offsets.Get(c.groupID, partition)
		if min < 0 || v < min {
			min = v
		}
	}
	return min
}

// Publish appends a message, routed by key hash (FNV-1a mod partitions).
// An empty key routes round-robin. It returns the partition and offset
// assigned. When BlockPublish is false and the target partition is at
// capacity it returns ErrQueueFull; when true it blocks (up to
// PublishTimeout) for capacity.
func (b *Bus) Publish(key, id string, payload []byte) (int, int64, error) {
	var part int
	if key == "" {
		part = int(b.rrCounter.Add(1)-1) % len(b.partitions)
	} else {
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		part = int(h.Sum32()) % len(b.partitions)
	}
	p := b.partitions[part]
	msg := Message{ID: id, Key: key, Payload: payload}

	// First gate: once draining/closed, new messages are never accepted.
	if s := b.state.Load(); s != stateOpen {
		return 0, 0, b.refuseError(s)
	}

	// blocking: park in WaitRoom until capacity frees.
	if b.cfg.BlockPublish {
		ctx := context.Background()
		if b.cfg.PublishTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, b.cfg.PublishTimeout)
			defer cancel()
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-b.drainCh:
				cancel()
			case <-ctx.Done():
			}
		}()
		if err := p.WaitRoom(ctx); err != nil {
			s := b.state.Load()
			if s != stateOpen || errors.Is(err, ErrBusClosed) {
				return 0, 0, b.refuseError(s)
			}
			if b.cfg.PublishTimeout > 0 {
				return 0, 0, ErrQueueFull
			}
			return 0, 0, err
		}
	}

	// Second gate: shutdown may have started while we parked (blocking) or
	// even between the first gate and append (non-blocking). Returning an
	// error here never loses data: this publisher had not appended yet.
	if s := b.state.Load(); s != stateOpen {
		return 0, 0, b.refuseError(s)
	}

	stored, err := p.TryAppend(msg)
	if err != nil {
		if errors.Is(err, ErrBusClosed) {
			return 0, 0, b.refuseError(b.state.Load())
		}
		return 0, 0, err
	}
	return part, stored.Offset, nil
}

// refuseError maps a lifecycle state to the stable error a Publish or
// Subscribe caller observes after shutdown has started.
func (b *Bus) refuseError(s uint32) error {
	if s == stateClosed {
		return &BusClosedError{Stage: stageClosed}
	}
	return &BusClosedError{Stage: stageDraining}
}

// fenced reports whether the drain window is over: after this point no
// commit or DLQ write may touch storage.
func (b *Bus) fenced() bool {
	select {
	case <-b.deadlineCh:
		return true
	default:
		return false
	}
}

// drainSignal returns a channel that is closed (and atomically replaced)
// whenever a commit might have moved a group closer to fully drained,
// letting Shutdown wake early. Both read and swap are atomic, so the
// commit path needs no bus.mu (avoids an ackMu -> bus.mu lock edge).
func (b *Bus) drainSignal() <-chan struct{} { return *b.drainNotify.Load() }

func (b *Bus) notifyDrain() {
	next := make(chan struct{})
	old := b.drainNotify.Swap(&next)
	close(*old)
}

// Subscribe registers a consumer group with the given handler and options.
// Group IDs are independent: each has its own offsets and receives every
// message. Calling Run on the returned Consumer starts delivery.
func (b *Bus) Subscribe(groupID string, h Handler, opts ...ConsumerOption) (*Consumer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.state.Load(); s != stateOpen {
		return nil, b.refuseError(s)
	}
	if _, exists := b.groups[groupID]; exists {
		return nil, &groupExistsError{group: groupID}
	}
	cfg := ConsumerConfig{
		MaxAttempts: 3,
		Backoff:     DefaultBackoff,
	}
	for _, o := range opts {
		o(&cfg)
	}
	c := &Consumer{
		bus:        b,
		groupID:    groupID,
		handler:    h,
		cfg:        cfg,
		watermarks: map[int]int64{},
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	b.groups[groupID] = c
	return c, nil
}

type groupExistsError struct{ group string }

func (e *groupExistsError) Error() string {
	return "bus: consumer group already subscribed: " + e.group
}

// DLQEntries returns every dead-letter entry across all groups.
func (b *Bus) DLQEntries() ([]DLQEntry, error) { return b.dlq.Entries() }

// Close is the legacy hard-stop: it is equivalent to Shutdown with no
// drain window, which unblocks publishers/consumers and releases storage
// immediately. Prefer Shutdown for the unified graceful shutdown.
func (b *Bus) Close() error {
	_, err := b.shutdown(0, 0)
	return err
}

func (b *Bus) closeStorage() error {
	var firstErr error
	for _, w := range b.wals {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := b.dlq.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := b.offsets.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
