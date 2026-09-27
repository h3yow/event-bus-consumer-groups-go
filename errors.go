package eventbus

import "errors"

// 可区分的哨兵错误，调用方可通过 errors.Is 判断拒绝/失败原因。
var (
	// ErrBusClosed 总线已关闭，拒绝新的生产与订阅。
	ErrBusClosed = errors.New("eventbus: bus is closed")
	// ErrQueueFull 消费队列达到容量上限，生产者被拒绝（非阻塞模式）。
	ErrQueueFull = errors.New("eventbus: consumer queue is full")
	// ErrOffsetRollback 位点回退被明确拒绝：新位点不得小于已提交位点。
	ErrOffsetRollback = errors.New("eventbus: offset rollback is rejected")
	// ErrOffsetOutOfRange 位点超出分区日志范围。
	ErrOffsetOutOfRange = errors.New("eventbus: offset out of range")
	// ErrMaxRetriesExceeded 重试次数耗尽，消息转入死信。
	ErrMaxRetriesExceeded = errors.New("eventbus: max retry attempts exceeded")
	// ErrNoHandler 订阅组未提供处理函数。
	ErrNoHandler = errors.New("eventbus: nil message handler")
	// ErrGroupExists 同一消费组重复订阅。
	ErrGroupExists = errors.New("eventbus: consumer group already exists")
)

// FailReason 标记死信消息的可区分失败原因。
type FailReason string

const (
	ReasonHandlerError FailReason = "handler_error"
	ReasonHandlerPanic FailReason = "handler_panic"
)
