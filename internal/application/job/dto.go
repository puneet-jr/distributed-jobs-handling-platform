package job

import domainjob "github.com/your-org/distributed-job-platform/internal/domain/job"

type CreateJobRequest struct {
	Type           string
	Payload        []byte
	Priority       int
	IdempotencyKey string
}

type CreateJobResponse struct {
	JobID  string           `json:"jobId"`
	Status domainjob.Status `json:"status"`
}

type GetJobResponse struct {
	ID           string           `json:"id"`
	Type         string           `json:"type"`
	Status       domainjob.Status `json:"status"`
	Priority     int              `json:"priority"`
	RetryCount   int              `json:"retryCount"`
	ErrorMessage *string          `json:"errorMessage,omitempty"`
}
