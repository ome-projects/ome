package ociobjectstore

import "fmt"

// WriteLimiter bounds concurrent model-file writes across all objects and
// model download tasks in one model-agent process.
type WriteLimiter struct {
	permits chan struct{}
}

// NewWriteLimiter creates a limiter with the given positive concurrency.
func NewWriteLimiter(concurrency int) (*WriteLimiter, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("model-file write concurrency must be greater than zero, got %d", concurrency)
	}
	return &WriteLimiter{permits: make(chan struct{}, concurrency)}, nil
}

func (limiter *WriteLimiter) acquire() {
	if limiter != nil {
		limiter.permits <- struct{}{}
	}
}

func (limiter *WriteLimiter) release() {
	if limiter != nil {
		<-limiter.permits
	}
}

// Limit returns the maximum number of concurrent model-file writes.
func (limiter *WriteLimiter) Limit() int {
	if limiter == nil {
		return 0
	}
	return cap(limiter.permits)
}
