package opinionatedevents

import "fmt"

type testDestinationHandler = func(msg *Message) error

type testDestination struct {
	handlers []testDestinationHandler
}

func (d *testDestination) deliver(msg *Message) error {
	handler, err := d.nextHandler()
	if err != nil {
		return err
	}

	return handler(msg)
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

func newTestDestination() *testDestination {
	return &testDestination{
		handlers: []testDestinationHandler{},
	}
}
