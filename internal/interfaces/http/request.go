package httpapi

import "encoding/json"

type CreateJobRequest struct {
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority"`
}
