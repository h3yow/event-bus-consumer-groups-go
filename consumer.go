package eventbus

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Handler 处理一条消息，返回 error 表示失败需要重试。
type Handler func(ctx context.Context, msg Message) error

// reconcileInterval 是没有新信号时回头检查持久化日志的周期（兜底背压/通知丢失）。
const reconcileInterval = 25 * time.Millisecond

// partitionConsumer 负责一个（消费组, 分区）。
//
// 设计要点：
//   - 分区内只有一个消费 goroutine，严格按 Offset 顺序处理并提交，保证有序且不重复；
//   - 投递只有单一事实来源——持久化分区日志。新消息通过 signal 通知，
//     即使通知因队列满被拒绝或进程重启，消费循环也会按 next 位点从日志补齐，不丢不重；
//   - signal 通道有界形成背压：满时 push 返回 ErrQueueFull（消息已落盘，稍后仍会投递）。
type partitionConsumer struct {
	group     string
	partition Partition
	cfg       Config
	logStore  *partitionLog
	offsets   *offsetStore
	dlq       *dlq
	logger    *log.Logger
	handler   Handler
	next      Offset

	signal chan struct{}

	done chan struct{}
}

func newPartitionConsumer(
	group string, p Partition, cfg Config,
	store *partitionLog, offsets *offsetStore, deadLetter *dlq,
	logger *log.Logger, h Handler, startAt Offset,
) *partitionConsumer {
	return &partitionConsumer{
		group:     group,
		partition: p,
		cfg:       cfg,
		logStore:  store,
		offsets:   offsets,
		dlq:       deadLetter,
		logger:    logger,
		handler:   h,
		next:      startAt,
		signal:    make(chan struct{}, cfg.PartitionQueueCapacity),
		done:      make(chan struct{}),
	}
}

// notify 告知该分区日志中可能有新消息。
// block=false：信号槽满立即返回 ErrQueueFull（消息已持久化，不丢，稍后由轮询兜底）。
// block=true：阻塞直到信号入槽或 ctx 取消。
func (pc *partitionConsumer) notify(ctx context.Context, block bool) error {
	if block {
		select {
		case pc.signal <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case pc.signal <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("%w: group=%s partition=%d capacity=%d",
			ErrQueueFull, pc.group, pc.partition, pc.cfg.PartitionQueueCapacity)
	}
}

// run 是该分区唯一的消费 goroutine。
func (pc *partitionConsumer) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(pc.done)
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		// 处理日志中所有 next 之后的消息（包括首次启动的重放与背压缺口）。
		for pc.next < pc.logStore.endOffset() {
			if ctx.Err() != nil {
				return
			}
			m, err := pc.logStore.readAt(pc.next)
			if err != nil {
				pc.logger.Printf("[%s] partition=%d read log at %d failed: %v",
					pc.group, pc.partition, pc.next, err)
				if !pc.sleep(ctx, pc.cfg.Retry.InitialBackoff) {
					return
				}
				continue
			}
			m.Partition = pc.partition
			pc.process(ctx, m)
		}
		// 没有可处理消息：等待新信号、兜底轮询或关闭。
		select {
		case <-ctx.Done():
			return
		case <-pc.signal:
		case <-ticker.C:
		}
	}
}

// process 处理单条消息：按退避策略重试，超限进 DLQ；成功（或入 DLQ）后提交位点。
func (pc *partitionConsumer) process(ctx context.Context, m Message) {
	policy := pc.cfg.Retry
	var lastErr error
	reason := ReasonHandlerError

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return // 关闭中：不提交，重启后从已提交位点继续
		}
		m.Attempt = attempt
		err := pc.invoke(ctx, m)
		if err == nil {
			pc.commit(m)
			return
		}
		lastErr = err
		if _, ok := err.(handlerPanic); ok {
			reason = ReasonHandlerPanic
		}
		pc.logger.Printf("[%s] partition=%d msg=%s offset=%d attempt=%d failed: %v",
			pc.group, pc.partition, m.ID, m.Offset, attempt, err)
		if attempt < policy.MaxAttempts {
			if !pc.sleep(ctx, policy.backoff(attempt)) {
				return
			}
		}
	}

	// 重试耗尽：进入死信，绝不静默丢弃；然后提交位点避免无限重投。
	record := DLQRecord{
		Message:   m,
		Reason:    reason,
		LastError: lastErr.Error(),
		Attempts:  policy.MaxAttempts,
	}
	if err := pc.dlq.add(record); err != nil {
		// 死信落盘失败时不提交位点，重启后重新处理，保证不丢。
		pc.logger.Printf("[%s] partition=%d msg=%s offset=%d dlq persist failed: %v",
			pc.group, pc.partition, m.ID, m.Offset, err)
		return
	}
	pc.logger.Printf("[%s] partition=%d msg=%s offset=%d -> DLQ reason=%s attempts=%d lastErr=%q",
		pc.group, pc.partition, m.ID, m.Offset, reason, policy.MaxAttempts, lastErr.Error())
	pc.commit(m)
}

type handlerPanic struct{ v interface{} }

func (e handlerPanic) Error() string { return fmt.Sprintf("handler panic: %v", e.v) }

func (pc *partitionConsumer) invoke(ctx context.Context, m Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = handlerPanic{v: r}
		}
	}()
	return pc.handler(ctx, m)
}

// commit 提交位点并推进内存位置。位点回退由 offsetStore 明确拒绝。
func (pc *partitionConsumer) commit(m Message) {
	if err := pc.offsets.commit(pc.group, pc.partition, m.Offset); err != nil {
		pc.logger.Printf("[%s] partition=%d msg=%s commit offset=%d rejected: %v",
			pc.group, pc.partition, m.ID, m.Offset, err)
		return
	}
	prev := pc.next
	pc.next = m.Offset + 1
	pc.logger.Printf("[%s] partition=%d msg=%s committed offset %d -> %d (next=%d)",
		pc.group, pc.partition, m.ID, prev, m.Offset, pc.next)
}

func (pc *partitionConsumer) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// consumerGroup 聚合一个组在所有分区上的消费者。
type consumerGroup struct {
	name    string
	pcs     []*partitionConsumer
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	offsets *offsetStore
}

func newConsumerGroup(
	name string, cfg Config, logs []*partitionLog,
	offsets *offsetStore, deadLetter *dlq, logger *log.Logger, h Handler,
) (*consumerGroup, error) {
	if h == nil {
		return nil, ErrNoHandler
	}
	g := &consumerGroup{name: name, offsets: offsets}
	for p := 0; p < cfg.Partitions; p++ {
		committed, err := offsets.get(name, Partition(p))
		if err != nil {
			return nil, err
		}
		startAt := Offset(0)
		if committed != InvalidOffset {
			startAt = committed + 1
		}
		pc := newPartitionConsumer(name, Partition(p), cfg, logs[p], offsets,
			deadLetter, logger, h, startAt)
		g.pcs = append(g.pcs, pc)
		logger.Printf("[%s] partition=%d resumed from committed=%d, next=%d",
			name, p, committed, startAt)
	}
	return g, nil
}

// start 启动各分区消费循环；恢复完全由循环按已提交位点从日志读取完成。
func (g *consumerGroup) start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	g.cancel = cancel
	for _, pc := range g.pcs {
		g.wg.Add(1)
		go pc.run(ctx, &g.wg)
	}
	return nil
}

func (g *consumerGroup) notify(ctx context.Context, m Message, block bool) error {
	p := int(m.Partition)
	if p < 0 || p >= len(g.pcs) {
		return ErrOffsetOutOfRange
	}
	return g.pcs[p].notify(ctx, block)
}

func (g *consumerGroup) close() {
	if g.cancel != nil {
		g.cancel()
	}
	g.wg.Wait()
}
