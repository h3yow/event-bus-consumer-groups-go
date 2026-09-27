package eventbus

import (
	"fmt"
)

// Offset 是消息在分区内的单调递增位点，从 0 开始。
type Offset int64

// InvalidOffset 表示尚无已提交位点（新消费组首次启动）。
const InvalidOffset Offset = -1

// Partition 分区编号。
type Partition int

// Message 是总线上投递的一条消息。
type Message struct {
	ID        string
	Key       string
	Payload   []byte
	Partition Partition
	Offset    Offset
	// Attempt 当前第几次投递（从 1 开始），便于处理器与日志判断重试。
	Attempt int
}

// DLQRecord 是进入死信队列的消息记录，保留失败原因与最后错误。
type DLQRecord struct {
	Message   Message
	Reason    FailReason
	LastError string
	Attempts  int
}

// Err 返回该死信记录对应的错误。
// 死信均由重试耗尽产生，因此返回的错误可通过 errors.Is 判断 ErrMaxRetriesExceeded。
func (r DLQRecord) Err() error {
	if r.LastError == "" {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrMaxRetriesExceeded, r.LastError)
}
