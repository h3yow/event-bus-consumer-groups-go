package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestOffsetPersistedAcrossReopen verifies the offset file itself is
// durable and readable after the store is closed and re-opened.
func TestOffsetPersistedAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	s, err := OpenOffsetStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("g", 2, 7); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("offset file missing: %v", err)
	}

	s2, err := OpenOffsetStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.Get("g", 2); got != 7 {
		t.Fatalf("reloaded offset=%d, want 7", got)
	}
	// Regression is still rejected after reload.
	err = s2.Commit("g", 2, 6)
	if !errors.Is(err, ErrOffsetRejected) {
		t.Fatalf("reloaded regression: want ErrOffsetRejected, got %v", err)
	}
	t.Logf("regression after reload rejected: %v", err)
	s2.Close()
}

// TestWALDurability replays a partition WAL after reopen: messages are
// restored with contiguous offsets and identical payloads/IDs.
func TestWALDurability(t *testing.T) {
	dir := t.TempDir()
	w, records, err := OpenWAL(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("fresh WAL has %d records", len(records))
	}
	want := []Message{
		{ID: "a", Key: "k", Partition: 3, Offset: 0, Payload: []byte("1")},
		{ID: "b", Key: "k", Partition: 3, Offset: 1, Payload: []byte("22")},
	}
	for _, m := range want {
		if err := w.Append(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	_, replay, err := OpenWAL(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != len(want) {
		t.Fatalf("replayed %d records, want %d", len(replay), len(want))
	}
	for i := range want {
		if replay[i].ID != want[i].ID || replay[i].Offset != want[i].Offset ||
			string(replay[i].Payload) != string(want[i].Payload) {
			t.Errorf("replay[%d]=%+v, want %+v", i, replay[i], want[i])
		}
		t.Logf("replayed id=%s partition=%d offset=%d", replay[i].ID, replay[i].Partition, replay[i].Offset)
	}
}

// TestPublishBeforeSubscribeDoesNotBlock verifies messages can be
// preloaded without any group attached (no capacity pressure), and are
// all delivered once a group subscribes.
func TestPublishBeforeSubscribe(t *testing.T) {
	cfg := baseCfg()
	cfg.QueueCapacity = 5 // small bound must not apply pre-subscription
	b, _ := newTestBus(t, cfg)
	const n = 30
	for i := 0; i < n; i++ {
		mustPublish(t, b, "k", fmt.Sprintf("m%d", i), nil)
	}
	var seen atomic.Int64
	c, _ := b.Subscribe("late", HandlerFunc(func(ctx context.Context, m Message) error {
		seen.Add(1)
		return nil
	}))
	go func() { _ = c.Run(context.Background()) }()
	waitFor(t, testTimeout, func() bool { return seen.Load() == int64(n) }, "late group gets preloaded backlog")
	t.Logf("late group consumed preloaded backlog=%d", seen.Load())
	c.Stop()
	c.Wait()
}

// TestExponentialBackoffBounds checks the configured backoff curve.
func TestExponentialBackoffBounds(t *testing.T) {
	b := ExponentialBackoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, Multiplier: 2}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 100 * time.Millisecond}, // capped
		{99, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := b.Delay(tc.attempt); got != tc.want {
			t.Errorf("attempt %d delay=%s want %s", tc.attempt, got, tc.want)
		}
	}
}
