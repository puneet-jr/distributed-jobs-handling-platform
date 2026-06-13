package executor 

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"distributed-job-platform/internal/jobs/model"
)

type EmailExecutor struct{}

func NewEmailExecutor() *EmailExecutor {
	return &EmailExecutor{}
}

func(e *EmailExecutor) Execute(ctx context.Context, payload []byte) error {
	var email model.EmailPayload
	if err := json.Unmarshal(payload, &email); err != nil {
		return fmt.Errorf("Failed to unmarshall email payload: %w",err)
	}

	fmt.Printf("[EmailExecutor] Sending email to %s with subject '%s'...\n",email.To,email.Subject)

	// Network delay for sending email

	select {
	case <- time.After(2* time.Second):
		fmt.Printf("[Email Executor] Email sent successfully to %s!\n",email.To)
		return nil
	case <- ctx.Done():
		return fmt.Errorf("Email execution cancelled: %w",ctx.Err())
	}
}