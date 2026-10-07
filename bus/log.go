package bus

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WAL is the per-partition write-ahead log on disk. Records are stored as
// one JSON object per line and fsynced before the message becomes visible
// in memory, so a crash never loses a published message.
type WAL struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

// walPath returns the WAL file name for a partition.
func walPath(dir string, partition int) string {
	return filepath.Join(dir, "partition-"+itoa(partition)+".wal")
}

// OpenWAL opens (or creates) a WAL file and replays existing records in order.
func OpenWAL(dir string, partition int) (*WAL, []Message, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := walPath(dir, partition)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	records, err := replayWAL(path)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, records, nil
}

func replayWAL(path string) ([]Message, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []Message
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i < len(raw) && raw[i] != '\n' {
			continue
		}
		line := raw[start:i]
		start = i + 1
		if len(line) == 0 {
			continue
		}
		var m Message
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// Append writes one record durably to the WAL (buffer flush + fsync).
func (w *WAL) Append(m Message) error {
	line, err := json.Marshal(m)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := w.w.Write(line); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close releases the WAL file.
func (w *WAL) Close() error { return w.f.Close() }

// PartitionLog is an in-memory append-only partition view backed by a WAL.
// Its capacity bounds the number of records not yet confirmed by the
// slowest subscribed consumer group (group lag); see Bus.Publish for the
// blocking/rejecting behaviour.
type PartitionLog struct {
	mu        sync.Mutex
	partition int
	capacity  int
	records   []Message
	// signal is closed and replaced whenever appends happen or the log closes,
	// waking blocked publishers/feeders (channel broadcast idiom).
	signal chan struct{}
	// sealed rejects new appends as soon as shutdown starts (distinct from
	// closed, which also stops consumers waiting on WaitOffset). Sealing
	// only blocks publishers; readers keep draining the backlog.
	sealed bool
	closed bool
	wal    *WAL

	// minCommitted reports the smallest "next offset" committed by any
	// subscribed group for this partition (0 if no group has committed).
	minCommitted func() int64
}

// newPartitionLog recreates a partition from replayed WAL records.
func newPartitionLog(partition, capacity int, wal *WAL, replay []Message, minCommitted func() int64) *PartitionLog {
	return &PartitionLog{
		partition:    partition,
		capacity:     capacity,
		records:      replay,
		signal:       make(chan struct{}),
		wal:          wal,
		minCommitted: minCommitted,
	}
}

// Signal returns a channel closed when the log changes (new append/close).
func (p *PartitionLog) Signal() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.signal
}

// Len returns the total number of stored records (next append offset).
func (p *PartitionLog) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.records)
}

// IsFull reports whether the bounded queue has room for one more record.
// Lag = stored records beyond the slowest group's committed watermark.
func (p *PartitionLog) IsFull() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fullLocked()
}

func (p *PartitionLog) fullLocked() bool {
	lag := int64(len(p.records))
	if p.minCommitted != nil {
		lag -= p.minCommitted()
	}
	if lag < 0 {
		lag = 0
	}
	return p.capacity > 0 && lag >= int64(p.capacity)
}

// WaitRoom blocks until the partition has capacity, ctx is cancelled or
// the log is closed.
func (p *PartitionLog) WaitRoom(ctx context.Context) error {
	for {
		p.mu.Lock()
		full, closed, sealed := p.fullLocked(), p.closed, p.sealed
		ch := p.signal
		p.mu.Unlock()
		if !full && !sealed {
			return nil
		}
		if sealed || closed {
			return ErrBusClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// WaitOffset blocks until a record at offset exists, the context is
// cancelled or the log is closed. The level check and the channel
// subscription happen under the same lock as append, so a wake-up can
// never be missed (monitor invariant).
func (p *PartitionLog) WaitOffset(ctx context.Context, offset int64) error {
	for {
		p.mu.Lock()
		has := offset < int64(len(p.records))
		closed := p.closed
		ch := p.signal
		p.mu.Unlock()
		if has {
			return nil
		}
		if closed {
			return ErrBusClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// TryAppend persists the message to the WAL then makes it visible at the
// next offset. Returns ErrQueueFull when the bounded queue is full and
// ErrBusClosed after close.
func (p *PartitionLog) TryAppend(m Message) (Message, error) {
	p.mu.Lock()
	if p.closed || p.sealed {
		p.mu.Unlock()
		return Message{}, ErrBusClosed
	}
	if p.fullLocked() {
		p.mu.Unlock()
		return Message{}, ErrQueueFull
	}
	m.Partition = p.partition
	m.Offset = int64(len(p.records))
	if m.EnqueuedAt.IsZero() {
		m.EnqueuedAt = time.Now()
	}
	// Durable BEFORE visible: crash between fsync and append would replay
	// the record from the WAL on restart.
	if err := p.wal.Append(m); err != nil {
		p.mu.Unlock()
		return Message{}, err
	}
	p.records = append(p.records, m)
	ch := p.signal
	p.signal = make(chan struct{})
	close(ch)
	p.mu.Unlock()
	return m, nil
}

// Get returns the record at offset (false if absent).
func (p *PartitionLog) Get(offset int64) (Message, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset < 0 || offset >= int64(len(p.records)) {
		return Message{}, false
	}
	return p.records[offset], true
}

// seal rejects new appends (and wakes publishers parked in WaitRoom) but
// leaves WaitOffset readers running so consumers can drain.
func (p *PartitionLog) seal() {
	p.mu.Lock()
	if !p.sealed {
		p.sealed = true
		ch := p.signal
		p.signal = make(chan struct{})
		close(ch)
	}
	p.mu.Unlock()
}

// close marks the log closed and wakes every waiter.
func (p *PartitionLog) close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		ch := p.signal
		p.signal = make(chan struct{})
		close(ch)
	}
	p.mu.Unlock()
}

// Notify wakes goroutines parked in WaitRoom/WaitOffset without appending
// (used when a committed offset frees capacity in the bounded queue).
func (p *PartitionLog) Notify() {
	p.mu.Lock()
	ch := p.signal
	p.signal = make(chan struct{})
	close(ch)
	p.mu.Unlock()
}
