package job

import "errors"

// Why custom errors instead of strings?
// errors.Is() and errors.As() allow type-safe error handling.
// Service layer can check: if errors.Is(err, ErrJobNotFound)
 
var (
 ErrJobNotFound = errors.New("job not found")
 ErrDuplicateIdempotencyKey = errors.New("Job with this idempotency key already exists")
ErrInvalidStatusTransition = errors.New("invalid status transition")
) 
