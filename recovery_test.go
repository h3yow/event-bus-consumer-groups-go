package eventbus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// restartBus 在同一 DataDir 上重建总线，模拟进程重启。
func restartBus(t *testing.T, cfg Config) *Bus {
	t.Helper()
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("reopen NewBus: %v", err)
	}
	return bus
}

// TestRestartResumeNoReplayConfirmed：
// 已确认位点持久化；重启后只处理崩溃前未确认的消息，已确认消息绝不重复消费。
func TestRestartResumeNoReplayConfirmed(t *testing.T) {
	cfg := testConfig(t)
	cfg.Partitions = 2
	dir := cfg.DataDir

	bus := restartBus(t, cfg)
	var mu sync.Mutex
	got := map[string]int{}
	barrier := make(chan struct{})
	if err := bus.Subscribe("g1", func(ctx context.Context, m Message) error {
		t.Logf("[run1] consume msg=%s partition=%d offset=%d", m.ID, m.Partition, m.Offset)
		mu.Lock()
		got[m.ID]++
		mu.Unlock()
		if m.ID == "msg-04" {
			close(barrier)
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("msg-%02d", i)
		if _, err := bus.Produce(ctx, Message{ID: id, Key: id}); err != nil {
			t.Fatalf("Produce %s: %v", id, err)
		}
	}
	<-barrier
	// 模拟崩溃：不等待消费完成即关闭（已提交位点已 fsync）。
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重启：同一 DataDir，新组订阅，统计重放了哪些消息。
	cfg.DataDir = dir
	bus2 := restartBus(t, cfg)
	defer bus2.Close()
	var mu2 sync.Mutex
	replayed := map[string]int{}
	if err := bus2.Subscribe("g1", func(ctx context.Context, m Message) error {
		t.Logf("[run2 after restart] consume msg=%s partition=%d offset=%d",
			m.ID, m.Partition, m.Offset)
		mu2.Lock()
		replayed[m.ID]++
		mu2.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe after restart: %v", err)
	}
	// 等待两轮合计覆盖全部 10 条消息（第一轮已确认的不再重放）。
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		n1 := len(got)
		mu.Unlock()
		mu2.Lock()
		n2 := len(replayed)
		mu2.Unlock()
		return n1+n2 == 10
	}) {
		t.Fatalf("restart replay did not converge: firstRun=%d replayed=%d",
			len(got), len(replayed))
	}

	// 崩溃前已处理的消息不得在重启后重复处理。
	mu.Lock()
	firstRun := map[string]int{}
	for k, v := range got {
		firstRun[k] = v
	}
	mu.Unlock()
	for id := range firstRun {
		if replayed[id] != 0 {
			t.Errorf("message %s was reprocessed after restart (must resume from committed offset)", id)
		}
	}
	// 每条消息在整个生命周期内恰好成功处理一次。
	for id, n := range replayed {
		if n > 1 {
			t.Errorf("message %s processed %d times after restart", id, n)
		}
		if _, wasFirst := firstRun[id]; wasFirst {
			t.Errorf("message %s counted in both runs", id)
		}
	}
}

// TestOffsetRollbackRejected：显式提交更小的位点必须被明确拒绝（ErrOffsetRollback），
// 不得静默接受；随后正常推进的位点仍然有效。
func TestOffsetRollbackRejected(t *testing.T) {
	cfg := testConfig(t)
	bus := restartBus(t, cfg)
	defer bus.Close()

	if err := bus.offsets.commit("grp", 0, 5); err != nil {
		t.Fatalf("commit 5: %v", err)
	}
	if err := bus.offsets.commit("grp", 0, 3); err == nil {
		t.Fatal("rollback commit 5->3 was silently accepted, want ErrOffsetRollback")
	} else {
		t.Logf("rollback rejected as expected: %v", err)
	}
	if off, err := bus.offsets.get("grp", 0); err != nil || off != 5 {
		t.Fatalf("after rejected rollback offset=%d err=%v, want 5", off, err)
	}
	if err := bus.offsets.commit("grp", 0, 6); err != nil {
		t.Fatalf("forward commit 6: %v", err)
	}
	if off, _ := bus.offsets.get("grp", 0); off != 6 {
		t.Fatalf("offset=%d want 6", off)
	}
}

// TestRestartPersistsOffsetsAndDLQ：位点与死信在重启后可从磁盘恢复。
func TestRestartPersistsOffsetsAndDLQ(t *testing.T) {
	cfg := fastRetryConfig(t)
	dir := cfg.DataDir
	bus := restartBus(t, cfg)
	if err := bus.Subscribe("g", func(ctx context.Context, m Message) error {
		return fmt.Errorf("always fail")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := bus.Produce(context.Background(), Message{ID: "poison", Key: "poison"}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if !waitFor(3*time.Second, func() bool { return len(bus.DLQ()) == 1 }) {
		t.Fatalf("DLQ=%d want 1", len(bus.DLQ()))
	}
	// 等位点提交
	waitFor(2*time.Second, func() bool {
		off, _ := bus.offsets.get("g", 0)
		return off == 0
	})
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cfg.DataDir = dir
	bus2 := restartBus(t, cfg)
	defer bus2.Close()
	if len(bus2.DLQ()) != 1 {
		t.Fatalf("DLQ after restart=%d want 1 (must not be silently lost)", len(bus2.DLQ()))
	}
	r := bus2.DLQ()[0]
	t.Logf("DLQ recovered msg=%s reason=%s lastErr=%q", r.Message.ID, r.Reason, r.LastError)
	if r.Message.ID != "poison" || r.Reason != ReasonHandlerError {
		t.Fatalf("unexpected recovered DLQ record: %+v", r)
	}
	off, err := bus2.offsets.get("g", 0)
	if err != nil || off != 0 {
		t.Fatalf("committed offset after restart=%d err=%v, want 0", off, err)
	}
}
