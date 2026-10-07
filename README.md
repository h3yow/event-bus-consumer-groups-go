# event-bus-consumer-groups-go

一个可在本地运行的事件驱动消息总线（纯 Go，无外部依赖），支持**多消费组、分区内有序投递、退避重试、死信队列、位点本地持久化与进程重启恢复**，并在队列满时提供可解释的背压行为。

- 语言：Go 1.25+（标准库实现）
- 运行示例：`go run ./cmd/demo`
- 运行测试：`go test ./... -race -v`

---

## 1. 快速上手

```go
b, _ := bus.New(bus.Config{
    DataDir:       "./busdata", // 持久化目录（WAL/位点/死信）
    Partitions:    4,
    QueueCapacity: 1024,
    BlockPublish:  true,
})

// 生产：相同 key 进入同一分区；空 key 轮询
partition, offset, err := b.Publish("order-42", "msg-001", payload)

// 两个消费组各自独立收到全量消息
c1, _ := b.Subscribe("payments", bus.HandlerFunc(func(ctx context.Context, m bus.Message) error {
    return process(m) // 返回 nil 视为成功
}), bus.WithMaxAttempts(5), bus.WithBackoff(bus.ExponentialBackoff{
    Base: 100 * time.Millisecond, Max: 10 * time.Second, Multiplier: 2,
}))

go c1.Run(context.Background()) // 阻塞运行；c1.Stop() 优雅停止
```

`cmd/demo/main.go` 是完整可运行示例：发布 12 条消息，一个组全量成功，另一个组对一条消息重试两次后成功、对一条消息立即死信，随后"重启进程"并断言已提交位点零重复投递。

---

## 2. 投递语义（Delivery Semantics）

**分区内严格有序 + 至少一次（at-least-once）投递，成功处理不重复。**

- 每个分区为一条 append-only 日志，消息有单调递增的 `Offset`。
- 每个消费组在**每个分区只有一个串行 feeder goroutine**：队首消息提交（ACK）之前，后续消息不会被处理，因此同一 key（同分区）天然有序。
- 不同分区由独立 goroutine 并发处理；多个消费组之间位点、投递状态完全独立，互不影响。
- 位点存储的是"**下一条待消费偏移量**"（next-offset 水位线，Kafka 风格）。只有在处理终态（成功或进入死信）后才会推进并持久化。
- 崩溃发生在"业务已成功、位点未及持久化"窗口时，重启后该消息会被再投递一次 —— 这是 at-least-once 的定义。**要求 Handler 幂等**。
- 成功路径的重复确认会被拒绝（`ErrDuplicateAck`），跳过未处理位移的确认会被拒绝（`ErrOutOfOrderAck`），因此同组同一条消息不可能被重复成功提交，也不会错误确认。

### 并发一致性

- 位点提交由 Consumer 内部的 `ackMu` 串行化，并与 `OffsetStore` 的互斥锁、单调校验配合；8 生产者 × 100 消息 × 8 分区的并发压测下断言：无丢失、无重复、无错位、每分区最终水位线等于日志长度（见 `TestConcurrentAckAcrossPartitions`，`-race` 下运行）。
- 等待新消息/等待容量均遵循 monitor 不变式：**水位检查与等待通道订阅在同一把分区锁内完成**，唤醒不会丢失，不会死锁。

---

## 3. 重试与死信规则（Retry & DLQ）

| 情况 | 行为 |
|---|---|
| Handler 返回 `nil` | 成功，位点推进 |
| Handler 返回普通 error | 按 `Backoff` 退避后重试 |
| Handler 返回 `bus.NonRetryable(err)` | **不重试**，立即进入 DLQ，原因 `non_retryable` |
| 重试次数达到 `MaxAttempts` 仍失败 | 进入 DLQ，原因 `max_retries_exceeded` |
| 进入 DLQ 后 | 位点照常推进，后续消息继续处理 —— **不会静默丢弃，也不会阻塞分区** |

- `MaxAttempts` **包含首次尝试**：`MaxAttempts=3` 表示最多 2 次重试（默认 3）。
- 退避曲线：`Base * Multiplier^(attempt-1)`，上限封顶 `Max`；默认 100ms→200ms→…→10s。
- DLQ 每条记录包含可机读的**可区分失败原因**、原始错误文本（`LastErr`）与尝试次数，以 JSON-lines 持久化在 `DataDir/dlq.log`，每次写入都 `fsync`：

```json
{"GroupID":"g","Message":{"ID":"poison",...},"Reason":"non_retryable","LastErr":"non-retryable: bad payload","Attempts":1}
{"GroupID":"g","Message":{...},"Reason":"max_retries_exceeded","LastErr":"boom","Attempts":3}
```

- 查询：`consumer.DLQEntries()`（按组过滤）或 `bus.DLQEntries()`（全量）。
- 停止（`Stop` / ctx 取消 / bus 关闭）发生在重试等待期间时，本次不提交位点，重启后重新投递。

---

## 4. 背压（Backpressure）

`QueueCapacity` 限制**每个分区中"已发布但尚未被最慢消费组提交"的消息积压（lag）**。容量按最慢组计算，因此一个慢组不会拖慢其他组的位点，但会对生产者施压 —— 这是真实消息队列的典型语义。

| 配置 | 队列满时的行为 |
|---|---|
| `BlockPublish=false` | 立即返回可解释的错误 `bus.ErrQueueFull`（"queue capacity reached, producer must back off"），由调用方决定退避 |
| `BlockPublish=true`（默认） | 阻塞直到有容量；可用 `PublishTimeout` 限制等待，超时同样返回 `ErrQueueFull` |
| bus 关闭 | 阻塞中的发布以 `ErrBusClosed` 唤醒，不会死锁 |

消费组提交位点释放容量后，阻塞的发布者会被精确唤醒（见 `TestBackpressureBlock` / `TestBackpressureBlockCloseUnblocks`）。

> 注：尚未有任何消费组订阅时不施加容量限制，允许先预灌数据再订阅（`TestPublishBeforeSubscribe`）。

---

## 5. 优雅停机（Graceful Shutdown）

`Shutdown(ctx)` 是统一的停机入口，替代"逐个 `Stop` 消费组再 `Close`"的手工流程：

```go
report, err := b.Shutdown(context.Background())
// report: 每个消费组 × 每个分区的已提交位点、未排空条数、DLQ 条数、
// 总体结果（drained / timeout）与原因
```

停机分四个阶段，**总耗时以 `DrainTimeout`（另加极小的 goroutine 退出宽限）为上界，绝不无限等待**：

1. **拒新**：所有分区被 seal。新 `Publish`/`Subscribe` 立即返回 `ErrBusDraining`；正阻塞在满队列上的生产者被唤醒并返回 `ErrBusDraining`（不会误报为 `ErrQueueFull`）。消费者读取不受影响，继续排空。
2. **排空**：在 `DrainTimeout` 窗口内等待所有消费组把"处理中、退避重试中、未确认"的消息推进到终态（成功提交或进 DLQ）。窗口内全部完成 → `Outcome=drained`；窗口到期仍有未确认 → `Outcome=timeout`。
3. **停止**：向所有消费组发停止信号并等待退出（有界等待；handler 卡死不会拖住停机）。
4. **落盘关闭**：从已持久化的位点、WAL 长度与 DLQ 生成 `ShutdownReport`，原子写入 `DataDir/shutdown-report.json`，然后关闭存储。

语义保证：

- **位点只进不退**：停机窗口内已确认的位置只会保持或前进；未确认的消息重启后照常重新投递（at-least-once 不变）。
- **不重复成功确认**：同组同一条消息的重复 ACK 仍被 `ErrDuplicateAck` 拒绝，停机路径不放宽该约束。
- **幂等**：多处并发调用 `Shutdown` 安全，所有调用者拿到**同一份**报告；完成后再次调用立即返回同一报告。
- **原因可区分**：排空中 → `ErrBusDraining`；停机完成后 → `ErrBusClosed`；容量背压 → `ErrQueueFull`。三者互不混淆。
- **报告可解释且与持久化状态一致**：`ShutdownReport` 完全由 `offsets.json`、WAL、DLQ 的持久化状态生成并落盘；重启后用 `LastShutdown()` 读回，与位点文件不会自相矛盾。

### 与原有停止方式的兼容范围

| 方式 | 语义 | 何时用 |
|---|---|---|
| `consumer.Stop()` + `bus.Close()` | 原有手工流程，仍然可用；`Close` 立即拒新并关闭存储，**不等待排空** | 需要强制立即退出 |
| `bus.Shutdown(ctx)` | 统一的拒新 + 有界排空 + 停止 + 关闭，产出可解释报告 | 正常停机（推荐） |

组合规则：`Shutdown` 之后再 `Close` 返回 `ErrBusClosed`（幂等安全）；`Close` 之后再 `Shutdown` 同样返回 `ErrBusClosed`。停机期间未确认的消息在下次 `New` 后由消费组从持久化位点重新投递。

### 本地验证

```bash
# 全量测试（含竞态检测）：停机相关用例在 bus/shutdown_test.go
go test ./... -race -v

# 重点组合场景（满队列 + 阻塞生产者 + 重试中消费者 + 并发停机）连跑 10 遍：
go test ./bus/ -run TestShutdownFullQueueBlockedProducersRetryingConsumer -count=10 -race -v
```

`shutdown_test.go` 覆盖：窗口内正常排空、排空中拒新（`ErrBusDraining`）、超时中断与重启重投、并发/重复停机幂等、`Close` 与 `Shutdown` 的组合，以及上述满队列压力组合（断言不丢消息、同一消息不重复成功确认、停机在时限内返回、报告与 `offsets.json` 一致）。`-v` 日志中可看到消息 ID、分区、offset 与水位线变化；停机后可直接查看 `DataDir/shutdown-report.json` 与 `offsets.json` 互相印证。

---

## 6. 故障恢复（Recovery）

`DataDir` 下有三类持久化文件，`bus.New` 启动时统一重放：

```
busdata/
├── partition-0.wal ... partition-N.wal  # 每分区消息 WAL（JSON-lines，逐条 fsync）
├── offsets.json                         # 消费组位点（原子快照：tmp + fsync + rename）
├── dlq.log                              # 死信记录（JSON-lines，逐条 fsync）
└── shutdown-report.json                 # 最近一次优雅停机报告（原子写入，重启后 LastShutdown 可读）
```

规则：

1. **消息不丢**：`Publish` 先把消息 fsync 到 WAL，再使其对消费者可见。
2. **已提交不重放**：消费组从 `offsets.json` 的 next-offset 恢复；已确认的消息重启后不会再投递（`TestRestartRecovery`）。
3. **未提交必重放**：处理中/重试中的消息（位点未推进）重启后从旧偏移重新投递（`TestRestartRedeliversUncommitted`）。
4. **位点只进不退**：任何把水位线调小的提交都会被显式拒绝 —— 返回包装了 `bus.ErrOffsetRejected` 的 `OffsetRegressionError`（携带组、分区、当前值与请求值），**绝不静默接受**；重启加载后该约束依然有效（`TestOffsetRegressionRejected` / `TestOffsetPersistedAcrossReopen`）。相等值的提交是幂等 no-op。

崩溃边界总结：

| 崩溃时刻 | 结果 |
|---|---|
| WAL fsync 之前 | 发布返回错误，消息不可见，无半条消息 |
| WAL fsync 之后、位点提交之前 | 重启后重新投递（at-least-once） |
| 位点原子快照完成之后 | 重启后从下一条继续，不重复 |

---

## 7. 配置项参考

### `bus.Config`

| 字段 | 默认 | 说明 |
|---|---|---|
| `DataDir` | `busdata` | WAL、`offsets.json`、`dlq.log` 的存放目录 |
| `Partitions` | `4` | 分区数；重启时必须与原有数据一致 |
| `QueueCapacity` | `1024` | 每分区按最慢组计算的积压上限；`0` 表示不限 |
| `BlockPublish` | `true` | 满队列时阻塞（`true`）还是立即拒绝 `ErrQueueFull` |
| `PublishTimeout` | `0`（无限等待） | 阻塞发布的最长等待时间，超时返回 `ErrQueueFull` |
| `DrainTimeout` | `5s` | 优雅停机的排空窗口：`Shutdown` 等待在途/重试中/未确认消息收尾的最长时间（`<=0` 取默认值） |

### 消费组选项

| 选项 | 默认 | 说明 |
|---|---|---|
| `WithMaxAttempts(n)` | `3` | 最大尝试次数（含首次） |
| `WithBackoff(p)` | 指数 100ms→10s | 重试退避策略，可自定义实现 `BackoffPolicy` 接口 |
| `WithLogger(l)` | 标准 logger | 记录投递、重试、位点变化与 DLQ（单测中同时打印消息 ID 与 offset） |

### 错误一览

| 错误 | 含义 |
|---|---|
| `ErrQueueFull` | 分区达到容量上限（拒绝模式 / 阻塞超时） |
| `ErrBusDraining` | 优雅停机进行中：拒新但仍在排空（发布/订阅/被唤醒的阻塞发布） |
| `ErrBusClosed` | bus 关闭后的操作（含停机完成后的发布/订阅） |
| `ErrOffsetRejected` | 位点回退被拒绝（包装为 `*OffsetRegressionError`） |
| `ErrDuplicateAck` | 同一消息被重复确认 |
| `ErrOutOfOrderAck` | 确认会跳过尚未提交的消息 |

---

## 8. 日志与可解释性

Consumer 的每条关键状态变化都有结构化文本日志，包含消息标识与位点变化，例如：

```
group=g id=always-fail partition=0 offset=0 attempt=2 failed: boom
group=g id=always-fail partition=0 offset=0 retry in 2ms (attempt 3/3)
group=g id=always-fail partition=0 offset=0 DLQ reason=max_retries_exceeded attempts=3 committed_next=1
group=g id=poison partition=0 offset=1 DLQ reason=non_retryable attempts=1 committed_next=2
group=g id=after-dlq partition=0 offset=2 attempt=1 ACK committed_next=3
group=g partition=0 resume at offset=3   # 重启恢复点
```

单测通过 `WithLogger` 注入测试 logger，在 `go test -v` 输出中可直接看到消息 ID、分区、offset 与水位线变化。

---

## 9. 适用边界

这是一个**单进程、本地持久化**的实现，用于清晰展示消费组/位点/重试/DLQ/恢复的核心机制与可测试语义；不包含网络协议、多副本选举与跨节点协调。若业务处理可能产生副作用，请使 Handler 幂等（at-least-once 的标准要求）。
