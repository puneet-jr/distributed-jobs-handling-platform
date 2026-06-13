package executor

import (
	"context"
)

type JobExecutor interface {
	Execute(ctx context.Context, payload []byte) error
}
