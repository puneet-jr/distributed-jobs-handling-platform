package observability

import (
	"time"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsCreated
	JobsStarted
	JobsCompleted
	
}