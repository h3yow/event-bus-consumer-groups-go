package eventbus

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// appendReq 是发往分区追加 worker 的请求。
type appendReq struct {
	msg   *Message
	ready chan appendResult
}

type appendResult struct {
	off Offset
	err error
}

// partitionAppender 串行化一个分区上的日志追加。
// 生产者在有界 channel 上排队（阻塞或快速拒绝），不直接争抢文件锁，
// 从而避免高并发下的锁护送与调度饥饿。
type partitionAppender struct {
	log    *partitionLog
	reqs   chan appendReq
	stop   chan struct{}
	done   chan struct{}
	logger *log.Logger

	closeMu sync.RWMutex
	closed  bool
}

func newPartitionAppender(l *partitionLog, capacity int, logger *log.Logger) *partitionAppender {
	a := &partitionAppender{
		log:    l,
		reqs:   make(chan appendReq, capacity),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		logger: logger,
	}
	go a.run()
	return a
}

func (a *partitionAppender) run() {
	for {
		select {
		case <-a.stop:
			// 关闭：排空已入队请求，避免阻塞中的 Produce 永久等待。
			for {
				select {
				case req := <-a.reqs:
					req.ready <- appendResult{err: ErrBusClosed}
				default:
					close(a.done)
					return
				}
			}
		case req := <-a.reqs:
			off, err := a.log.append(req.msg)
			req.ready <- appendResult{off: off, err: err}
		}
	}
}

// submit 入队一个追加请求；返回 queued=false 表示非阻塞模式下队列已满。
// 总线关闭后明确返回 ErrBusClosed。
func (a *partitionAppender) submit(ctx context.Context, req appendReq, block bool) (bool, error) {
	a.closeMu.RLock()
	closed := a.closed
	a.closeMu.RUnlock()
	if closed {
		return false, ErrBusClosed
	}
	if block {
		select {
		case a.reqs <- req:
			return true, nil
		case <-a.stop:
			return false, ErrBusClosed
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	select {
	case a.reqs <- req:
		return true, nil
	case <-a.stop:
		return false, ErrBusClosed
	default:
		return false, nil
	}
}

// shutdown 标记关闭、停止接收并等待 worker 退出。
func (a *partitionAppender) shutdown() {
	a.closeMu.Lock()
	if !a.closed {
		a.closed = true
		close(a.stop)
	}
	a.closeMu.Unlock()
	<-a.done
}

// Bus 是事件驱动消息总线：管理分区持久化日志、消费组、位点与死信。
type Bus struct {
	cfg       Config
	logger    *log.Logger
	logs      []*partitionLog
	appenders []*partitionAppender
	offsets   *offsetStore
	dlq       *dlq

	mu     sync.RWMutex
	closed bool
	groups map[string]*consumerGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// NewBus 根据配置创建总线；DataDir 中已有数据时会自动打开（用于重启恢复）。
func NewBus(cfg Config) (*Bus, error) {
	cfg.normalize()
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("eventbus: DataDir is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "partitions"), 0o755); err != nil {
		return nil, err
	}
	logs := make([]*partitionLog, cfg.Partitions)
	for p := 0; p < cfg.Partitions; p++ {
		l, err := newPartitionLog(cfg.DataDir, Partition(p))
		if err != nil {
			return nil, err
		}
		logs[p] = l
	}
	offsets, err := newOffsetStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	deadLetter, err := newDLQ(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bus{
		cfg:     cfg,
		logger:  log.New(os.Stderr, "eventbus ", log.LstdFlags|log.Lmicroseconds),
		logs:    logs,
		offsets: offsets,
		dlq:     deadLetter,
		groups:  map[string]*consumerGroup{},
		ctx:     ctx,
		cancel:  cancel,
	}
	appenders := make([]*partitionAppender, cfg.Partitions)
	for p, l := range logs {
		appenders[p] = newPartitionAppender(l, cfg.PartitionQueueCapacity, b.logger)
	}
	b.appenders = appenders
	return b, nil
}

// SetLogger 允许调用方替换日志输出（默认丢弃；测试可按需开启）。
func (b *Bus) SetLogger(w io.Writer) {
	b.logger = log.New(w, "eventbus ", log.LstdFlags|log.Lmicroseconds)
}

// Produce 写入一条消息并返回带分区/位点的最终消息。
//
// 持久化与背压：
//   - 消息通过分区追加 worker 串行 fsync 落盘；worker 请求队列容量为
//     PartitionQueueCapacity；
//   - BlockWhenFull=false（默认）：该分区在途追加队列满时立即返回 ErrQueueFull，
//     这是一种可解释的拒绝——消息尚未入队，调用方可退避后重试；
//   - BlockWhenFull=true：阻塞直到进入追加队列（受 ctx 控制）。
//
// 落盘成功后以 best-effort 方式唤醒各消费组；若某组在途信号槽已满，
// 丢弃该次通知也不影响正确性——消费循环会以固定周期从持久化日志协调补齐，消息不丢。
func (b *Bus) Produce(ctx context.Context, msg Message) (Message, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return Message{}, ErrBusClosed
	}
	if msg.Key == "" {
		msg.Key = msg.ID
	}
	msg.Partition = partitionFor(msg.Key, b.cfg.Partitions)
	appender := b.appenders[msg.Partition]
	groups := make([]*consumerGroup, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}
	block := b.cfg.BlockWhenFull
	b.mu.RUnlock()

	req := appendReq{msg: &msg, ready: make(chan appendResult, 1)}
	queued, err := appender.submit(ctx, req, block)
	if err != nil {
		return Message{}, err
	}
	if !queued {
		return Message{}, fmt.Errorf("%w: partition=%d capacity=%d",
			ErrQueueFull, msg.Partition, b.cfg.PartitionQueueCapacity)
	}
	res := <-req.ready
	if res.err != nil {
		return Message{}, fmt.Errorf("append partition log: %w", res.err)
	}
	msg.Offset = res.off
	b.logger.Printf("produce msg=%s partition=%d offset=%d", msg.ID, msg.Partition, msg.Offset)

	// best-effort 唤醒消费组；通知丢失由消费循环的日志协调机制兜底。
	for _, g := range groups {
		_ = g.notify(ctx, msg, false)
	}
	return msg, nil
}

// Subscribe 注册一个消费组并立即启动；重启时从已提交位点恢复，重放未确认消息。
func (b *Bus) Subscribe(group string, h Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBusClosed
	}
	if _, exists := b.groups[group]; exists {
		return fmt.Errorf("%w: %s", ErrGroupExists, group)
	}
	g, err := newConsumerGroup(group, b.cfg, b.logs, b.offsets, b.dlq, b.logger, h)
	if err != nil {
		return err
	}
	if err := g.start(b.ctx); err != nil {
		g.close()
		return err
	}
	b.groups[group] = g
	return nil
}

// DLQ 返回死信记录快照（含历史重启前落盘的记录）。
func (b *Bus) DLQ() []DLQRecord { return b.dlq.snapshot() }

// Close 停止所有消费组与追加 worker、刷盘并关闭文件。
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	groups := make([]*consumerGroup, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}
	appenders := b.appenders
	b.mu.Unlock()

	b.cancel()
	var firstErr error
	// 先停止追加 worker：解除所有在 Produce 上阻塞的生产者，避免关闭死锁。
	for _, a := range appenders {
		a.shutdown()
	}
	// 再停止消费组（ctx 已取消，尊重 ctx 的处理器会退出；不尊重的处理器无法被强杀）。
	for _, g := range groups {
		g.close()
	}
	if err := b.dlq.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, l := range b.logs {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// partitionFor 依据 Key 的 FNV-1a 哈希选分区，保证同一 Key 总是进入同一分区。
func partitionFor(key string, n int) Partition {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return Partition(int(h.Sum32()) % n)
}
