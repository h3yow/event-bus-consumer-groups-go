// Command demo runs a local end-to-end scenario:
//
//  1. publish N messages;
//  2. one group processes everything; another group retries one message
//     and dead-letters a non-retryable one;
//  3. the process "restarts": the bus is re-opened from the same DataDir
//     and groups resume from persisted offsets with zero redelivery.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/event-bus-consumer-groups-go/bus"
)

func main() {
	dir, err := os.MkdirTemp("", "eventbus-demo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := bus.Config{
		DataDir:       dir,
		Partitions:    3,
		QueueCapacity: 64,
		BlockPublish:  false,
	}

	// ---- phase 1: first run ----
	b, err := bus.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	const total = 12
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("msg-%02d", i)
		key := fmt.Sprintf("key-%d", i%4)
		part, off, err := b.Publish(key, id, []byte(fmt.Sprintf(`{"n":%d}`, i)))
		if err != nil {
			log.Fatalf("publish %s: %v", id, err)
		}
		log.Printf("published id=%s partition=%d offset=%d", id, part, off)
	}

	var seenA, seenB atomic.Int64
	grpA, err := b.Subscribe("group-a", bus.HandlerFunc(func(ctx context.Context, m bus.Message) error {
		seenA.Add(1)
		log.Printf("[group-a] handled id=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		return nil
	}))
	if err != nil {
		log.Fatal(err)
	}

	var failMu sync.Mutex
	failCount := map[string]int{}
	grpB, err := b.Subscribe("group-b", bus.HandlerFunc(func(ctx context.Context, m bus.Message) error {
		if m.ID == "msg-07" {
			return bus.NonRetryable(fmt.Errorf("poison payload"))
		}
		if m.ID == "msg-03" {
			failMu.Lock()
			failCount[m.ID]++
			n := failCount[m.ID]
			failMu.Unlock()
			if n <= 2 {
				return fmt.Errorf("transient failure #%d", n)
			}
		}
		seenB.Add(1)
		log.Printf("[group-b] handled id=%s partition=%d offset=%d attempt=%d",
			m.ID, m.Partition, m.Offset, m.Attempt)
		return nil
	}), bus.WithMaxAttempts(4),
		bus.WithBackoff(bus.ExponentialBackoff{Base: 5 * time.Millisecond, Max: 50 * time.Millisecond, Multiplier: 2}),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	go func() {
		if err := grpA.Run(ctx); err != nil {
			log.Printf("group-a run: %v", err)
		}
	}()
	go func() {
		if err := grpB.Run(ctx); err != nil {
			log.Printf("group-b run: %v", err)
		}
	}()

	waitUntil(func() bool { return seenA.Load() >= total }, 3*time.Second, "group-a consumes all")
	// group-b: total-1 handled successfully, 1 dead-lettered.
	var bDLQ []bus.DLQEntry
	waitUntil(func() bool {
		bDLQ, _ = grpB.DLQEntries()
		return seenB.Load() >= total-1 && len(bDLQ) >= 1
	}, 3*time.Second, "group-b consumes with one DLQ")

	grpA.Stop()
	grpB.Stop()
	grpA.Wait()
	grpB.Wait()
	if err := b.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("phase 1 done: group-b successes=%d DLQ=%d reason=%s lastErr=%q",
		seenB.Load(), len(bDLQ), bDLQ[0].Reason, bDLQ[0].LastErr)

	// ---- phase 2: process restart ----
	b2, err := bus.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer b2.Close()

	var redelivered atomic.Int64
	grpA2, _ := b2.Subscribe("group-a", bus.HandlerFunc(func(ctx context.Context, m bus.Message) error {
		redelivered.Add(1) // must stay 0: group-a already committed everything
		log.Printf("[group-a after restart] id=%s offset=%d", m.ID, m.Offset)
		return nil
	}))
	grpB2, _ := b2.Subscribe("group-b", bus.HandlerFunc(func(ctx context.Context, m bus.Message) error {
		redelivered.Add(1) // must stay 0: all messages committed (incl. DLQ)
		log.Printf("[group-b after restart] id=%s offset=%d", m.ID, m.Offset)
		return nil
	}))
	go func() { _ = grpA2.Run(ctx) }()
	go func() { _ = grpB2.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	grpA2.Stop()
	grpB2.Stop()
	grpA2.Wait()
	grpB2.Wait()

	if redelivered.Load() != 0 {
		log.Fatalf("RESTART UNSAFE: %d messages redelivered after committed offsets", redelivered.Load())
	}
	log.Printf("phase 2 done: %d redelivered messages after restart (expected 0)", redelivered.Load())
}

func waitUntil(cond func() bool, timeout time.Duration, what string) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Fatalf("timeout waiting: %s", what)
}
