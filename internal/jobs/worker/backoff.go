package worker

import (
	"math"
	"time"
)

type BackoffStrategy interface {
	Calculate(attempt int) time.Duration
}

type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay time.Duration
	Factor float64
}

func NewExponentialBackoff(base, max time.Duration,factor float64) *ExponentialBackoff {
	return &ExponentialBackoff{BaseDelay: base , MaxDelay: max, Factor: factor}
}

// Calculate returns the delay for the next retry. 
// Attempt 1 = BaseDelay, Attempt 2 = BaseDelay * Factor, etc.
func (b *ExponentialBackoff) Calculate(attempt int) time.Duration {
	delay := float64(b.BaseDelay) * math.Pow(b.Factor, float64(attempt-1))
	if delay > float64(b.MaxDelay) {
		return b.MaxDelay
	}
	return time.Duration(delay)
}
