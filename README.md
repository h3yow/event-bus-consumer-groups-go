# event-bus-consumer-groups-go

一个可在本地运行的事件驱动消息总线：支持多消费组、分区有序投递、失败重试、
死信队列、位点本地持久化与进程重启恢复。仅依赖 Go 标准库，数据保存在本地目录，
无需任何外部中间件。

## 快速开始

```go
cfg := eventbus.DefaultConfig()
cfg.DataDir = "./data"

bus, err := eventbus.NewBus(cfg)
if err != nil { log.Fatal(err) }
defer bus.Close()

// 注册消费组（同名组在同一进程内只能订阅一次）
err = bus.Subscribe("orders", func(ctx context.Context, m eventbus.Message) error {
    fmt.Printf("group=orders msg=%s partition=%d offset=%d attempt=%d\n",
        m.ID, m.Partition, m.Offset, m.Attempt)
    return nil // 返回 error 触发重试
})

// 生产消息：Key 相同的消息总在同一分区
stored, err := bus.Produce(ctx, eventbus.Message{
    ID:   "msg-001",
    Key:  "user-42",
    Payload: []byte(`{"amount":100}`),
})
```

运行测试（含并发竞争、重试、死信、重启恢复场景）：

```sh
go test -race ./...
```

## 投递语义

- **分区**：消息按 `Key` 的 FNV-1a 哈希固定到一个分区（`Key` 为空时用 `ID`）。
- **消费组**：每个消费组独立维护各自的分区位点；组与组之间互不影响，
  同一条消息会被每个组各消费一次。
- **分区内有序、恰好一次成功处理**：每个（组, 分区）只有一个消费 goroutine，
  严格按位点顺序处理；处理成功后才提交位点。同一组对同一条消息不会重复
  成功处理（处理器在提交前崩溃属于 at-least-once 重试，重启会重新执行该消息）。
- **持久化日志**：消息先写入分区追加日志并 `fsync`，再通知消费者。
  日志帧格式为 `4 字节大端长度 + JSON 负载`，位点即记录在分区文件中的追加序号（从 0 起）。
- **单一投递路径**：消费循环只以「持久化日志 + 已提交位点」为权威来源顺序读取；
  生产成功后的内存通知仅用于低延迟唤醒，通知丢失（如背压、进程重启）时，
  消费循环会周期性（25ms）从日志协调补齐，因此通知路径永不造成丢消息或重复。

## 重试与死信规则

- 处理器返回 `error` 或发生 `panic`（会被捕获，不会击垮消费者）即视为失败，
  按 `RetryPolicy` 退避重试，退避可被总线关闭中断。
- 退避公式：`InitialBackoff * Multiplier^(attempt-1)`，上限 `MaxBackoff`。
- 重试次数（含首次）达到 `MaxAttempts` 仍失败后，消息进入**死信队列**，绝不静默丢弃：
  - 记录字段：消息原文、`Reason`、`LastError`、尝试次数；
  - 失败原因可区分：
    - `ReasonHandlerError`（`"handler_error"`）：处理器返回普通错误；
    - `ReasonHandlerPanic`（`"handler_panic"`）：处理器 panic。
  - 死信记录的 `LastError` 以 `ErrMaxRetriesExceeded` 为前缀（可 `errors.Is`
    判断），后附处理器最后一次返回的错误文本。
  - 死信同时保存在内存与 `dlq.jsonl`（每行一条 JSON，`fsync` 落盘），重启后可通过 `Bus.DLQ()` 重新读出。
- 死信落盘成功后才提交该消息位点，保证分区处理继续向后推进且不丢消息；
  若死信落盘本身失败，则不提交位点，重启后重新处理。

## 位点提交与故障恢复

- 位点保存在 `offsets/group-<组名>/partition-<分区号>.offset`，内容为纯整数；
  每个（组, 分区）独立文件、独立锁，提交采用「临时文件 + fsync + 原子 rename」，
  崩溃后文件要么是旧内容要么是新内容，不会出现半写状态。
- 位点在处理器成功（或消息入死信）后立即持久化。
- **重启行为**：重新 `NewBus` 同一 `DataDir` 时：
  - 分区日志从磁盘重建位点索引；
  - 消费组从已提交位点之后的位置继续；已确认的消息不会重复消费，
    崩溃前未确认的消息会从日志重放。
- **位点回退被明确拒绝**：提交小于已确认值的位点返回
  `ErrOffsetRollback`（可用 `errors.Is` 判断），不会静默接受，已确认值保持不变。

## 背压与并发

- 每个分区有一个串行的追加 worker 和容量为 `PartitionQueueCapacity` 的有界在途队列。
- `BlockWhenFull=false`（默认）：队列满时 `Produce` 立即返回可区分的
  `ErrQueueFull`（`errors.Is(err, ErrQueueFull)`）。这是一种**可解释的拒绝**：
  消息尚未进入持久化队列，调用方应退避后重试；不会死锁、不会影响其它消息。
- `BlockWhenFull=true`：`Produce` 阻塞等待空位（受传入 `ctx` 控制），
  适合要求生产端自动限流的场景。
- 消费者通知为 best-effort：即使某消费组信号槽已满而丢弃本次通知，
  消息也已在持久化日志中，消费循环的周期协调会将其补齐。
- 所有共享状态（日志句柄、位点、死信、组注册表）均由互斥量或单 goroutine
  串行保护，`go test -race ./...` 持续通过。

## 配置项

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `DataDir` | 本地持久化目录（分区日志、位点、死信），不存在自动创建 | `"eventbus-data"` |
| `Partitions` | 分区数量，必须 ≥ 1 | `2` |
| `PartitionQueueCapacity` | 每分区在途追加/信号队列容量，用于背压 | `128` |
| `BlockWhenFull` | 队列满时是阻塞（true）还是立即返回 `ErrQueueFull`（false） | `false` |
| `Retry.MaxAttempts` | 最大处理尝试次数（含首次） | `3` |
| `Retry.InitialBackoff` | 首次失败后退避时长 | `10ms` |
| `Retry.MaxBackoff` | 退避上限 | `100ms` |
| `Retry.Multiplier` | 退避乘数（指数退避，<1 视为 1） | `2` |

## 可区分错误

| 哨兵错误 | 含义 |
| --- | --- |
| `ErrQueueFull` | 在途队列达到容量上限（非阻塞模式的可解释拒绝） |
| `ErrBusClosed` | 总线已关闭，拒绝生产/订阅 |
| `ErrOffsetRollback` | 位点回退被明确拒绝 |
| `ErrOffsetOutOfRange` | 位点超出分区日志范围 |
| `ErrMaxRetriesExceeded` | 重试耗尽（进入死信） |
| `ErrNoHandler` | 订阅未提供处理函数 |
| `ErrGroupExists` | 同名消费组重复订阅 |

## 磁盘布局

```
<DataDir>/
├── partitions/
│   ├── partition-0.log
│   └── partition-1.log
├── offsets/
│   └── group-<组名>/
│       ├── partition-0.offset
│       └── partition-1.offset
└── dlq.jsonl
```
