package job

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
	StatusCancelled Status = "cancelled"
)

func (s Status) IsTerminal() bool {
	return s == StatusCompleted ||
		s == StatusFailed ||
		s == StatusCancelled
}

func CanTransition(from Status, to Status) bool {
	if from == to {
		return true
	}

	switch from {
	case StatusPending:
		return to == StatusRunning || to == StatusCancelled

	case StatusRunning:
		return to == StatusCompleted ||
			to == StatusFailed ||
			to == StatusRetrying ||
			to == StatusCancelled

	case StatusRetrying:
		return to == StatusPending ||
			to == StatusRunning ||
			to == StatusFailed ||
			to == StatusCancelled

	case StatusCompleted, StatusFailed, StatusCancelled:
		return false

	default:
		return false
	}
}
