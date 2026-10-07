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

## 4.5 统一优雅停机（Graceful Shutdown）

`Bus.Shutdown(...)` 把"逐个停消费组 + 关总线"合并为**一个有界、可预期、可解释**的动作，替代手工编排。

### 停机时序与语义

1. **立即停止接收新工作**：状态从 `open` 切到 `draining`，此后 `Publish` / `Subscribe` 立即被拒绝；因队列满而阻塞在 `WaitRoom` 的生产者被立刻唤醒。
2. **排空窗口（drain window）**：给"正在执行的 handler、退避重试中的消息、尚未确认的积压"一段可配置时间继续收尾。窗口内成功的处理照常提交并 fsync 持久化；一旦所有运行中的组把每个分区都追到日志末端，**提前结束**，不必等满窗口。
3. **截止围栏（fence）**：窗口结束即关闭 deadline，投递循环被取消，重试等待被打断；此后任何 `Commit` / DLQ 写入都被硬围栏拒绝（`ErrShutdownFenced`）——**存储释放后绝不可能再被写入**。
4. **停止宽限（stop grace）**：窗口结束瞬间仍在 handler 里的调用，再给一个很短的有界宽限等待其返回；超时也不影响停机按时结束（其结果会被围栏丢弃）。
5. 关闭分区日志、释放 WAL/DLQ/offset 句柄，并把停机报告**原子持久化**到 `DataDir/shutdown.json`。

**确认语义不被破坏**：

- 已确认位点只能**保持或前进**（沿用 `OffsetStore` 的单调校验，回退一律拒绝）。
- 窗口结束时仍未确认的消息**不提交**，下次启动从旧位点重新投递（at-least-once）。
- 同一消费组同一条消息不会出现两次成功确认：窗口后的"迟到成功"被围栏挡下，位点不动，重启只再投一次。
- 阻塞中的生产者要么在围栏前进了容量并成功落 WAL（消息不丢），要么收到明确的拒绝错误自行重试——**不存在静默丢失**。

### 停机后的稳定、可区分原因

| 调用时机 | 返回错误 |
|---|---|
| 排空中（`draining`） | `*BusClosedError{Stage:"draining"}`，同时 `errors.Is(err, bus.ErrBusShuttingDown)` 和 `errors.Is(err, bus.ErrBusClosed)` 都为真 |
| 停机完成后（`closed`） | `*BusClosedError{Stage:"closed"}`，只匹配 `bus.ErrBusClosed`，**不**匹配 `ErrBusShuttingDown` |

因此调用方既能用 `ErrBusClosed` 统一判断"总线不再可用"，也能用 `ErrBusShuttingDown` 区分"排空阶段被拒"与"已彻底关闭"。

**并发与重复调用安全幂等**：多个 goroutine 同时调用 `Shutdown`、或对已停机的总线重复调用，都安全。只有第一个调用执行编排，其余调用等待结束并拿到**同一份内容**的报告（副本），且不返回错误。停机一定在 `DrainTimeout + StopGrace`（加少量调度余量）内返回，不会无限等待退避重试或卡死的生产者/handler。

### 停机报告（ShutdownReport）

按**消费组 × 分区**列出停机时刻的可解释结果，全部数字在循环静止后从已持久化状态读取，与 `offsets.json` / `dlq.log` 一致；**重启后用 `bus.LastShutdownReport()` 再查也不会自相矛盾**。

```go
rep, err := b.Shutdown(
    bus.WithDrainTimeout(5*time.Second),
    bus.WithStopGrace(250*time.Millisecond),
)
// rep.Outcome: "drained"（窗口内全部收尾）或 "timeout"（有未排空工作）
// rep.Reason : 总线级中断原因
for _, g := range rep.Groups {
    // g.Outcome / g.Reason / g.Pending / g.NewDLQ
    for _, p := range g.Partitions {
        // p.Partition / p.CommittedNext / p.LogEndOffset / p.Pending / p.NewDLQ
        // p.InterruptReason
    }
}
```

字段含义：

| 字段 | 含义 |
|---|---|
| `CommittedNext` | 该组该分区已持久化的"下一条待消费"位点 |
| `LogEndOffset` | 分区 WAL 当前日志末端 |
| `Pending` | 未排空消息数 = `LogEndOffset - CommittedNext`（重启后重新投递） |
| `NewDLQ` | 本次停机窗口内**新增**的死信条数（已持久化、位点已推进） |
| `InterruptReason` | `drained` / `drain_timeout`（在重试/追赶但超时）/ `handler_stuck`（宽限结束 handler 仍未返回）/ `not_running`（订阅了但没在跑） |

报告示例（一个分区排空、一个分区在重试中被中断）：

```json
{
  "outcome": "timeout",
  "reason": "drain_timeout",
  "groups": [
    {"group_id": "g", "outcome": "interrupted", "pending": 1, "new_dlq": 0, "reason": "drain_timeout",
     "partitions": [
       {"partition": 0, "committed_next": 5, "log_end_offset": 5, "pending": 0, "new_dlq": 0},
       {"partition": 1, "committed_next": 3, "log_end_offset": 4, "pending": 1, "new_dlq": 0,
        "interrupt_reason": "drain_timeout"}
     ]}
  ]
}
```

### 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `Config.ShutdownTimeout` | `5s` | 默认排空窗口；可用 `WithDrainTimeout` 按次覆盖 |
| `Config.StopGrace` | `250ms` | 窗口结束后等待在途 handler 返回的宽限；可用 `WithStopGrace` 覆盖 |
| `Config.Logger` | 标准 logger | 记录停机生命周期与围栏事件（消息 ID/分区/位点） |

### 与旧的"逐个停消费组"方式的兼容范围

- `Consumer.Stop()` / `Consumer.Wait()` / `ctx` 取消**仍然保留且语义不变**，可以继续单独使用；你仍可先手工停各组再 `Close`。
- `Bus.Close()` 现在等价于 `Shutdown` 的**零排空窗口**形态（立即围栏、释放存储），保持原有"硬关闭"语义；旧代码无需改动。
- 新代码推荐统一调用一次 `Shutdown`：它内部会停止所有组、等待有界宽限、落盘报告并关闭总线，**不要**在 `Shutdown` 之后再调用 `Close`（幂等，直接返回缓存报告）。
- 建议：handler 尽量响应传入的 `ctx`（停机截止会取消它），以获得更快、更干净的排空；不响应 ctx 的 handler 由 `StopGrace` 兜底，不会拖死停机。

---

## 5. 故障恢复（Recovery）

`DataDir` 下有三类持久化文件，`bus.New` 启动时统一重放：

```
busdata/
├── partition-0.wal ... partition-N.wal  # 每分区消息 WAL（JSON-lines，逐条 fsync）
├── offsets.json                         # 消费组位点（原子快照：tmp + fsync + rename）
├── dlq.log                              # 死信记录（JSON-lines，逐条 fsync）
└── shutdown.json                        # 最近一次优雅停机报告（原子快照，可跨重启查询）
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

## 6. 配置项参考

### `bus.Config`

| 字段 | 默认 | 说明 |
|---|---|---|
| `DataDir` | `busdata` | WAL、`offsets.json`、`dlq.log` 的存放目录 |
| `Partitions` | `4` | 分区数；重启时必须与原有数据一致 |
| `QueueCapacity` | `1024` | 每分区按最慢组计算的积压上限；`0` 表示不限 |
| `BlockPublish` | `true` | 满队列时阻塞（`true`）还是立即拒绝 `ErrQueueFull` |
| `PublishTimeout` | `0`（无限等待） | 阻塞发布的最长等待时间，超时返回 `ErrQueueFull` |
| `ShutdownTimeout` | `5s` | `Shutdown` 默认排空窗口 |
| `StopGrace` | `250ms` | 排空截止后等待在途 handler 返回的宽限 |
| `Logger` | 标准 logger | 停机/投递/位点日志出口 |

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
| `ErrBusClosed` | bus 关闭后的操作 |
| `ErrOffsetRejected` | 位点回退被拒绝（包装为 `*OffsetRegressionError`） |
| `ErrDuplicateAck` | 同一消息被重复确认 |
| `ErrOutOfOrderAck` | 确认会跳过尚未提交的消息 |
| `ErrBusShuttingDown` | 优雅停机排空中拒绝新发布/订阅（同时匹配 `ErrBusClosed`） |
| `ErrShutdownFenced` | 排空截止后的迟到提交/DLQ 写入被围栏拒绝；消息保持未确认，重启重投 |

`*BusClosedError.Stage` 取值为 `"draining"` / `"closed"`，用于区分解雇阶段。停机报告见第 4.5 节，报告持久化在 `DataDir/shutdown.json`。

---

## 7. 日志与可解释性

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

## 9. 本地验证

```bash
# 全量用例（含新增停机用例），开启竞态检测
go test ./... -race -v

# 重点：有界队列打满 + 生产者等待 + 消费者重试 + 同时停机，多轮稳定性
go test ./bus/ -race -run TestShutdownComboStress -count=10 -v

# 其它停机用例：正常排空 / 超时围栏 / 错误可区分 / handler 卡死 / 窗口内 DLQ
go test ./bus/ -race -run TestShutdown -v

# 端到端示例（发布 -> 重试/DLQ -> “重启” -> 零重复）
go run ./cmd/demo
```

停机相关用例与覆盖点：

| 用例 | 覆盖 |
|---|---|
| `TestShutdownDrainsWithinWindow` | 窗口内全部收尾、报告 pending=0、重启零重投、报告与位点一致 |
| `TestShutdownTimeoutFencesCommit` | 窗口不足时超时返回、迟到提交被围栏、未确认消息重启重投、原因 `drain_timeout` |
| `TestShutdownRefusesNewWorkWithDistinctErrors` | 排空中 vs 关闭后错误可区分、阻塞生产者被唤醒、并发/重复停机幂等等待同一报告 |
| `TestShutdownHandlerStuckReason` | handler 无视 ctx 卡到宽限结束：按时返回、原因 `handler_stuck`、释放后 Ack 被围栏 |
| `TestShutdownDLQDuringDrain` | 窗口内进入 DLQ：持久化、位点推进、计入 `new_dlq`、重启不重投 |
| `TestShutdownComboStress`（40 小轮） | 队列满 + 生产者等待 + 消费者重试 + 双发停机的组合；断言有界返回、无丢失、全生命周期恰好一次成功、位点不回退 |

> 所有停机用例的日志（`-v` 输出）都带消息标识、分区、位点变化（如 `ACK-SUCCESS id=.. partition=.. offset=..`、`AFTER-RESTART id=.. offset=..`、`FENCED ... redelivery on restart`），便于复盘。

---

## 10. 适用边界

这是一个**单进程、本地持久化**的实现，用于清晰展示消费组/位点/重试/DLQ/恢复的核心机制与可测试语义；不包含网络协议、多副本选举与跨节点协调。若业务处理可能产生副作用，请使 Handler 幂等（at-least-once 的标准要求）。
