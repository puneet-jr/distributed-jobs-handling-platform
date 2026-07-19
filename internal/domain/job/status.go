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
