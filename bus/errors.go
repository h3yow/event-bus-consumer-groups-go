package bus

import "errors"

// Sentinel errors used across the bus.
var (
	// ErrBusClosed is returned when an operation is attempted after Close.
	ErrBusClosed = errors.New("bus: bus is closed")
	// ErrQueueFull is returned by non-blocking Publish when every partition
	// (or the target partition) is at capacity.
	ErrQueueFull = errors.New("bus: queue capacity reached, producer must back off")
	// ErrOffsetRejected is returned when a commit would move the committed
	// offset backwards. Regression is never silently accepted.
	ErrOffsetRejected = errors.New("bus: offset commit rejected: offset cannot regress")
	// ErrDuplicateAck is returned when a delivery is acknowledged more than once.
	ErrDuplicateAck = errors.New("bus: duplicate ack for message")
	// ErrOutOfOrderAck is returned when an ack would skip unprocessed offsets.
	ErrOutOfOrderAck = errors.New("bus: ack rejected: offset gap, earlier messages are not committed")
	// ErrUnknownGroup is returned when a group has no subscription/state.
	ErrUnknownGroup = errors.New("bus: unknown consumer group")
	// ErrBusDraining is returned by Publish while a graceful Shutdown is in
	// progress: new messages are no longer accepted, but already-published
	// messages are still being drained. It is distinguishable from
	// ErrBusClosed (shutdown fully completed) and ErrQueueFull (backpressure).
	ErrBusDraining = errors.New("bus: shutdown in progress, not accepting new messages")
)

// RejectReason distinguishes why a message entered the dead-letter queue.
type RejectReason string

const (
	// ReasonMaxRetriesExceeded: handler kept failing retryable errors beyond MaxAttempts.
	ReasonMaxRetriesExceeded RejectReason = "max_retries_exceeded"
	// ReasonNonRetryable: handler returned an error wrapped with NonRetryable.
	ReasonNonRetryable RejectReason = "non_retryable"
)

// NonRetryableError marks a handler error as permanently fatal: the message
// goes to the DLQ immediately without further retries.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string { return "non-retryable: " + e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable wraps err so the consumer does not retry the message.
func NonRetryable(err error) error {
	return &NonRetryableError{Err: err}
}

// DLQEntry is a record of a message that could not be processed.
type DLQEntry struct {
	GroupID  string
	Message  Message
	Reason   RejectReason
	LastErr  string
	Attempts int
}
