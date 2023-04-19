package opinionatedevents

import (
	"errors"
	"fmt"
	"time"
)

type retryError struct {
	retryAt time.Time
	err     error
}

func (f *retryError) Error() string {
	return f.err.Error()
}

func (f *retryError) Unwrap() error {
	return f.err
}

type fatalError struct {
	err error
}

func (f *fatalError) Error() string {
	return fmt.Sprintf("fatal: %s", f.err.Error())
}

func (f *fatalError) Unwrap() error {
	return f.err
}

func Fatal(err error) error {
	return &fatalError{err}
}

func IsFatal(err error) bool {
	var fatalErr *fatalError
	return errors.As(err, &fatalErr)
}
