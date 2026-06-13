package model

import (
    "encoding/json"
    "time"
)

type Job struct {
    ID        string          `json:"id" db:"id"`
    Type      JobType         `json:"type" db:"type"`
    Status    JobStatus       `json:"status" db:"status"`
    Payload   json.RawMessage `json:"payload" db:"payload"`
    CreatedAt time.Time       `json:"created_at" db:"created_at"`
    UpdatedAt time.Time       `json:"updated_at" db:"updated_at"`
}

type EmailPayload struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}
