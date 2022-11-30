package opinionatedevents

import "time"

type Result struct {
	Err     error
	RetryAt time.Time
}

type ResultContainer interface {
	GetResult() *Result
}

type successResult struct{}

func SuccessResult() *successResult {
	return &successResult{}
}

func (r *successResult) GetResult() *Result {
	var zero time.Time
	return &Result{Err: nil, RetryAt: zero}
}

type errorResult struct {
	err     error
	retryAt time.Time
}

func ErrorResult(err error, delay time.Duration) *errorResult {
	return &errorResult{err: err, retryAt: time.Now().Add(delay)}
}

func (r *errorResult) GetResult() *Result {
	return &Result{Err: r.err, RetryAt: r.retryAt}
}

type fatalResult struct {
	err error
}

func FatalResult(err error) *fatalResult {
	return &fatalResult{err: err}
}

func (r *fatalResult) GetResult() *Result {
	var zero time.Time
	return &Result{Err: r.err, RetryAt: zero}
}
