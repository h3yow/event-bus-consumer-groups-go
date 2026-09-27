package bus

import "time"

// Message is an immutable event as stored in a partition log.
type Message struct {
	ID         string
	Key        string
	Partition  int
	Offset     int64
	Payload    []byte
	Attempt    int // current processing attempt (1-based); 0 in the log
	EnqueuedAt time.Time
}
