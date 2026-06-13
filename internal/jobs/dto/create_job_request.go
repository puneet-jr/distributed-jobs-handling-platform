package dto
import (
	"encoding/json"
    "distributed-job-platform/internal/jobs/model"
)

type CreateJobRequest struct {
	Type model.JobType	`json:"type" binding:"required"`
	Payload json.RawMessage `json:"payload" binding:"required"`
}

type UpdateStatusRequest struct {
	Status model.Jobstatus `json:"status" bind:"required"`
}
