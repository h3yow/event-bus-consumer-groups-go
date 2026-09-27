package bus

import (
	"bytes"
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

// newTestBus creates a bus backed by a temporary directory.
func newTestBus(t *testing.T, cfg Config) (*Bus, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "eventbus-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if cfg.DataDir == "" {
		cfg.DataDir = dir
	}
	b, err := New(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, dir
}

func mustPublish(t *testing.T, b *Bus, key, id string, payload []byte) (int, int64) {
	t.Helper()
	p, o, err := b.Publish(key, id, payload)
	if err != nil {
		t.Fatalf("Publish(%s): %v", id, err)
	}
	return p, o
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

// testLogger returns a logger writing to both test log and an in-memory
// buffer so tests can assert on log output.
func testLogger(t *testing.T) (*log.Logger, *syncWriter) {
	w := &syncWriter{w: &bytes.Buffer{}, t: t}
	l := log.New(w, "", log.Ltime|log.Lmicroseconds)
	return l, w
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
	t  *testing.T
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.t.Logf("%s", bytes.TrimRight(p, "\n"))
	return s.w.Write(p)
}

// String returns the captured log output under lock (safe to call while
// the consumer goroutine is still logging).
func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.String()
}

var _ = context.Background
