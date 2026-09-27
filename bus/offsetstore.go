package bus

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// offsetSnapshot is the on-disk JSON format. The value is the NEXT offset
// to consume (standard high-watermark semantics). Keys are "<group>#<partition>".
type offsetSnapshot struct {
	Offsets map[string]int64 `json:"offsets"`
}

// OffsetStore persists per-group/per-partition committed offsets.
// The stored value is the NEXT offset to consume.
type OffsetStore struct {
	mu   sync.Mutex
	path string
	off  map[string]int64
}

func offsetMapKey(group string, partition int) string {
	return group + "#" + itoa(partition)
}

// OpenOffsetStore loads offsets from path, creating it if absent.
func OpenOffsetStore(path string) (*OffsetStore, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	s := &OffsetStore{path: path, off: map[string]int64{}}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	if len(raw) > 0 {
		var snap offsetSnapshot
		if err := json.Unmarshal(raw, &snap); err != nil {
			return nil, err
		}
		if snap.Offsets != nil {
			s.off = snap.Offsets
		}
	}
	return s, nil
}

// Get returns the committed "next offset" for a group/partition (0 if none).
func (s *OffsetStore) Get(group string, partition int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.off[offsetMapKey(group, partition)]
}

// Commit advances the stored offset for a group/partition.
// A commit lower than the current value is rejected with ErrOffsetRejected.
// Equal values are allowed (idempotent commit). Persistence is atomic:
// write temp file, fsync, rename over the real file.
func (s *OffsetStore) Commit(group string, partition int, nextOffset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := offsetMapKey(group, partition)
	cur := s.off[key]
	if nextOffset < cur {
		// Offset regression is explicitly rejected, never silently accepted.
		return &OffsetRegressionError{
			Group: group, Partition: partition,
			Current: cur, Requested: nextOffset,
		}
	}
	if nextOffset == cur {
		return nil
	}
	s.off[key] = nextOffset
	if err := s.persistLocked(); err != nil {
		// Keep memory and disk consistent: roll back the in-memory change.
		s.off[key] = cur
		return err
	}
	return nil
}

// OffsetRegressionError wraps ErrOffsetRejected with diagnostic details.
type OffsetRegressionError struct {
	Group     string
	Partition int
	Current   int64
	Requested int64
}

func (e *OffsetRegressionError) Error() string {
	return "bus: offset commit rejected: offset cannot regress " +
		"(group=" + e.Group + ", partition=" + itoa(e.Partition) +
		", current=" + itoa64(e.Current) + ", requested=" + itoa64(e.Requested) + ")"
}

func (e *OffsetRegressionError) Is(target error) bool {
	return errors.Is(target, ErrOffsetRejected) || target == ErrOffsetRejected
}

func (s *OffsetStore) persistLocked() error {
	snap := offsetSnapshot{Offsets: s.off}
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
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
	return os.Rename(tmp, s.path)
}

// Close releases the store (state is persisted on every commit).
func (s *OffsetStore) Close() error { return nil }
