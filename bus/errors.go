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
	// ErrBusShuttingDown is returned by Publish/Subscribe while a graceful
	// shutdown is draining, before storage is fully closed. It wraps
	// ErrBusClosed, so errors.Is(err, ErrBusClosed) still matches; match
	// this sentinel to distinguish "refused during drain" from "already
	// fully closed" (plain ErrBusClosed).
	ErrBusShuttingDown = errors.New("bus: shutdown in progress, refusing new work")
	// ErrShutdownFenced is returned when a commit/DLQ write is attempted
	// after the drain window closed. The write is refused so storage is
	// never touched after release; the message stays uncommitted and is
	// redelivered on the next start.
	ErrShutdownFenced = errors.New("bus: shutdown fence reached, commit refused; message will be redelivered")
)

// BusClosedError wraps ErrBusClosed with a stable, distinguishable stage:
// "draining" (graceful shutdown started, new work refused) or "closed"
// (shutdown finished and storage released).
type BusClosedError struct {
	Stage string
}

func (e *BusClosedError) Error() string {
	switch e.Stage {
	case stageDraining:
		return ErrBusShuttingDown.Error()
	default:
		return ErrBusClosed.Error()
	}
}

// Is makes both stages match ErrBusClosed and the draining stage also
// match ErrBusShuttingDown.
func (e *BusClosedError) Is(target error) bool {
	if target == ErrBusClosed {
		return true
	}
	if e.Stage == stageDraining && target == ErrBusShuttingDown {
		return true
	}
	return false
}

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
