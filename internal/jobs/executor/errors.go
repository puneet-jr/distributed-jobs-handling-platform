package executor

import "fmt"

// TransientError indicates a temporary failure (e.g., network timeout, 503 Service Unavailable).
// The worker should retry these.
type TransientError struct {
	Err error
}

func (e *TransientError) Error() string { return fmt.Sprintf("transient error: %v", e.Err) }
func (e *TransientError) Unwrap() error { return e.Err }

// PermanentError indicates a fatal failure (e.g., invalid payload, 400 Bad Request).
// The worker should NOT retry these.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return fmt.Sprintf("permanent error: %v", e.Err) }
func (e *PermanentError) Unwrap() error { return e.Err }