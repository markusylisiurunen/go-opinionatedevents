package opinionatedevents

import "time"

type Result interface {
	error() error
	retryAt() time.Time
}

// Error result
// ---

type errorResult struct {
	err        error
	retryDelay time.Duration
}

func (r *errorResult) error() error {
	return r.err
}

func (r *errorResult) retryAt() time.Time {
	if r.retryDelay.Nanoseconds() == 0 {
		var zero time.Time
		return zero
	}

	return time.Now().Add(r.retryDelay)
}

// Success result
// ---

type successResult struct{}

func (r *successResult) error() error {
	return nil
}

func (r *successResult) retryAt() time.Time {
	var zero time.Time
	return zero
}

// Builders
// ---

type errorResultOption func(r *errorResult)
type successResultOption func(r *successResult)

func ErrorResult(err error, opts ...errorResultOption) Result {
	result := &errorResult{err: err, retryDelay: 30 * time.Second}

	for _, applyOption := range opts {
		applyOption(result)
	}

	return result
}

func SuccessResult(opts ...successResultOption) Result {
	result := &successResult{}

	for _, applyOption := range opts {
		applyOption(result)
	}

	return result
}

func ResultWithNoRetries() errorResultOption {
	return func(r *errorResult) {
		r.retryDelay = 0 * time.Nanosecond
	}
}

func ResultWithRetryAfter(delay time.Duration) errorResultOption {
	return func(r *errorResult) {
		r.retryDelay = delay
	}
}
