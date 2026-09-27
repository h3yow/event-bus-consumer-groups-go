package bus

import "time"

// BackoffPolicy computes the delay before the nth retry.
// attempt is 1-based for the first retry.
type BackoffPolicy interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff implements Base * Multiplier^(attempt-1), capped at Max.
// Zero-valued fields are replaced by defaults: Base=100ms, Max=10s, Multiplier=2.
type ExponentialBackoff struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
}

// DefaultBackoff is the policy used when a consumer specifies none.
var DefaultBackoff = ExponentialBackoff{
	Base:       100 * time.Millisecond,
	Max:        10 * time.Second,
	Multiplier: 2,
}

func (e ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := e.Base
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	max := e.Max
	if max <= 0 {
		max = 10 * time.Second
	}
	mult := e.Multiplier
	if mult <= 1 {
		mult = 2
	}
	d := float64(base)
	for i := 1; i < attempt; i++ {
		d *= mult
		if d >= float64(max) {
			return max
		}
	}
	if d > float64(max) {
		d = float64(max)
	}
	return time.Duration(d)
}
