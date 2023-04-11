package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type testDestinationHandler = func(ctx context.Context, msg *Message) error

type testDestination struct {
	handlers []testDestinationHandler
}

func newTestDestination() *testDestination {
	return &testDestination{handlers: []testDestinationHandler{}}
}

func (d *testDestination) Deliver(ctx context.Context, msg *Message) error {
	if handler, err := d.nextHandler(); err != nil {
		return err
	} else {
		return handler(ctx, msg)
	}
}

func (d *testDestination) nextHandler() (testDestinationHandler, error) {
	if len(d.handlers) == 0 {
		return nil, fmt.Errorf("no handlers left")
	}
	handler := d.handlers[0]
	d.handlers = d.handlers[1:]
	return handler, nil
}

func (d *testDestination) pushHandler(handler testDestinationHandler) {
	d.handlers = append(d.handlers, handler)
}

func waitForSuccessEnvelope(envelope *envelope) error {
	select {
	case <-envelope.onSuccess():
		return nil
	case <-envelope.onFailure():
		return errors.New("failed")
	case <-time.After(1 * time.Second):
		return errors.New("timeout reached")
	}
}
