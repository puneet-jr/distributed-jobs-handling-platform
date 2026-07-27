package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	domainjob "distributed-job-platform/internal/domain/job"
)

type EmailHandler struct {
	logger *slog.Logger
}

func NewEmailHandler(logger *slog.Logger) *EmailHandler {
	return &EmailHandler{logger: logger}
}

func (h *EmailHandler) Handle(ctx context.Context, job domainjob.Job) error {
	var payload struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("invalid email payload: %w", err)
	}

	if payload.To == "" {
		return fmt.Errorf("Email payload requires to")
	}

	// This is where SMTP/provider code will go later.
	h.logger.Info("email job handled", "job_id", job.ID, "to", payload.To, "subject", payload.Subject)

	return nil
}

type PDFHandler struct {
	logger *slog.Logger
}

func NewPDFHandler(logger *slog.Logger) *PDFHandler {
	return &PDFHandler{logger: logger}
}

func (h *PDFHandler) Handle(ctx context.Context, job domainjob.Job) error {
	var payload struct {
		Template string `json:"template"`
	}

	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("invalid pdf payload: %w", err)
	}

	if payload.Template == "" {
		return fmt.Errorf("pdf payload requires template")
	}

	time.Sleep(500 * time.Millisecond)

	h.logger.Info("pdf job handled", "job_id", job.ID, "template", payload.Template)

	return nil
}
