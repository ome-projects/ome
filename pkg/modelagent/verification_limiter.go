package modelagent

// verificationLimiter bounds concurrent file integrity checks across all
// model downloads handled by one model-agent process.
type verificationLimiter struct {
	permits chan struct{}
}

func newVerificationLimiter(concurrency int) *verificationLimiter {
	if concurrency < 1 {
		concurrency = 1
	}
	return &verificationLimiter{permits: make(chan struct{}, concurrency)}
}

func (l *verificationLimiter) acquire() {
	l.permits <- struct{}{}
}

func (l *verificationLimiter) release() {
	<-l.permits
}

func (l *verificationLimiter) limit() int {
	return cap(l.permits)
}
