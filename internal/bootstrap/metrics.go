package bootstrap

import "sync/atomic"

type Metrics struct {
	HTTPRequests uint64
	JobsCreated  uint64
}

func (m *Metrics) IncHTTPRequests() { atomic.AddUint64(&m.HTTPRequests, 1) }
func (m *Metrics) IncJobsCreated()  { atomic.AddUint64(&m.JobsCreated, 1) }
