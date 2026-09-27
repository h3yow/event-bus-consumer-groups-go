package eventbus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestCrashRecoveryAcrossProcess：用子进程模拟“处理到一半直接 os.Exit”的硬崩溃，
// 重启后已提交位点不丢、不回退；未提交的消息重放，已提交消息不重复。
func TestCrashRecoveryAcrossProcess(t *testing.T) {
	if os.Getenv("EB_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}
	dir := filepath.Join(t.TempDir(), "bus")

	// 第一阶段：子进程处理消息，处理完前 5 条后硬退出（模拟崩溃）。
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryAcrossProcess")
	cmd.Env = append(os.Environ(), "EB_CRASH_CHILD=1", "EB_DATADIR="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil && cmd.ProcessState.ExitCode() != 7 {
		t.Fatalf("child crashed unexpectedly: %v\n%s", err, out)
	}
	t.Logf("child phase done, data dir=%s", dir)

	// 第二阶段：新进程重开同一目录，应从已提交位点继续。
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Partitions = 1
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bus.Close()

	var n int32
	if err := bus.Subscribe("crash-group", func(ctx context.Context, m Message) error {
		t.Logf("after-crash consume msg=%s offset=%d attempt=%d", m.ID, m.Offset, m.Attempt)
		if m.Offset < 5 {
			t.Errorf("msg=%s offset=%d was reprocessed after crash (committed offsets lost)", m.ID, m.Offset)
		}
		atomic.AddInt32(&n, 1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !waitFor(5*time.Second, func() bool { return atomic.LoadInt32(&n) == 5 }) {
		t.Fatalf("after crash consumed=%d want 5 (offs 5..9)", atomic.LoadInt32(&n))
	}
}

// runCrashChild 子进程：生产 10 条，处理完 offset 0..4 后立即 os.Exit(7) 模拟硬崩溃。
func runCrashChild() {
	dir := os.Getenv("EB_DATADIR")
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Partitions = 1
	cfg.BlockWhenFull = true
	bus, err := NewBus(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var n int32
	if err := bus.Subscribe("crash-group", func(ctx context.Context, m Message) error {
		atomic.AddInt32(&n, 1)
		return nil
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for i := 0; i < 10; i++ {
		if _, err := bus.Produce(context.Background(), Message{
			ID: fmt.Sprintf("m%02d", i), Key: fmt.Sprintf("k%02d", i),
		}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// 必须等到 offset=4 的位点真正 fsync 落盘后再模拟崩溃。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if off, _ := bus.offsets.get("crash-group", 0); off >= 4 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if off, _ := bus.offsets.get("crash-group", 0); off < 4 {
		fmt.Fprintln(os.Stderr, "offset 4 not committed before crash")
		os.Exit(1)
	}
	os.Exit(7) // 硬崩溃：不调用 bus.Close()
}

// TestCloseUnblocksProducer：阻塞模式下队列满时 Produce 在等待，
// Close 必须解除其阻塞并返回错误，不能死锁。
func TestCloseUnblocksProducer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "bus")
	cfg.Partitions = 1
	cfg.PartitionQueueCapacity = 1
	cfg.BlockWhenFull = true
	bus, err := NewBus(cfg)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	release := make(chan struct{})
	got := make(chan error, 16)
	if err := bus.Subscribe("g", func(ctx context.Context, m Message) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// 占满 worker 与容量为 1 的队列后，后续 Produce 阻塞。
	for i := 0; i < 6; i++ {
		go func(i int) {
			_, err := bus.Produce(context.Background(), Message{ID: fmt.Sprintf("m%d", i), Key: "k"})
			got <- err
		}(i)
	}
	time.Sleep(100 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		if err := bus.Close(); err != nil {
			fmt.Println("close err:", err)
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Close deadlocked while producers were blocking")
	}
	close(release)
	// 所有阻塞的生产者都应收到结果（ErrBusClosed 或成功），无 goroutine 泄漏/死锁。
	for i := 0; i < 6; i++ {
		select {
		case err := <-got:
			if err != nil && !errors.Is(err, ErrBusClosed) {
				t.Errorf("unexpected produce error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("producer %d remained blocked after Close", i)
		}
	}
}
