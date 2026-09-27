package bus

import (
	"context"
	"errors"
	"hash/fnv"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
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
	closed     bool
	closeCh    chan struct{}
	rrCounter  atomic.Uint64
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
		cfg:     cfg,
		groups:  map[string]*Consumer{},
		offsets: offsets,
		dlq:     dlq,
		closeCh: make(chan struct{}),
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
			case <-b.closeCh:
				cancel()
			case <-ctx.Done():
			}
		}()
		if err := p.WaitRoom(ctx); err != nil {
			b.mu.RLock()
			closed := b.closed
			b.mu.RUnlock()
			if closed || errors.Is(err, ErrBusClosed) {
				return 0, 0, ErrBusClosed
			}
			if b.cfg.PublishTimeout > 0 {
				return 0, 0, ErrQueueFull
			}
			return 0, 0, err
		}
	}

	stored, err := p.TryAppend(msg)
	if err != nil {
		return 0, 0, err
	}
	return part, stored.Offset, nil
}

// Subscribe registers a consumer group with the given handler and options.
// Group IDs are independent: each has its own offsets and receives every
// message. Calling Run on the returned Consumer starts delivery.
func (b *Bus) Subscribe(groupID string, h Handler, opts ...ConsumerOption) (*Consumer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBusClosed
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

// Close stops accepting publishes, wakes all blocked publishers and
// closes every storage handle. Consumers must be stopped separately
// before Close to finish in-flight work.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBusClosed
	}
	b.closed = true
	close(b.closeCh)
	for _, p := range b.partitions {
		p.close()
	}
	b.mu.Unlock()
	return b.closeStorage()
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
