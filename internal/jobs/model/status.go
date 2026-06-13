package model

type JobStatus string

const (
    StatusPending   JobStatus = "PENDING"
    StatusRunning   JobStatus = "RUNNING"
    StatusCompleted JobStatus = "COMPLETED"
    StatusFailed    JobStatus = "FAILED"
)

type JobType string

const (
    TypeEmail  JobType = "EMAIL"
    TypePDF    JobType = "PDF"
    TypeImage  JobType = "IMAGE"
    TypeReport JobType = "REPORT"
)
