package opinionatedevents

import (
	"context"
	"sync"
)

const (
	deliveryEventSuccessName string = "success"
	deliveryEventFailureName string = "failure"
)

type deliveryEvent struct {
	name string
}

func newDeliveryEvent(name string) *deliveryEvent {
	return &deliveryEvent{name: name}
}

type envelope struct {
	ctx   context.Context
	batch []*Message

	events chan *deliveryEvent

	proxies     []chan *deliveryEvent
	proxiesLock sync.Mutex

	closedWith     *deliveryEvent
	closedWithLock sync.Mutex
}

func (e *envelope) onEvent(name string) chan struct{} {
	in := make(chan *deliveryEvent)
	out := make(chan struct{})

	e.addProxy(in)

	go func() {
		for event := range in {
			if event.name == name {
				out <- struct{}{}
				break
			}
		}

		// TODO: should close the `out` channel
		// close(out)
	}()

	return out
}

func (e *envelope) onSuccess() chan struct{} {
	if e.isClosed() {
		out := make(chan struct{}, 1)

		if e.isClosedWith(deliveryEventSuccessName) {
			out <- struct{}{}
		}

		return out
	}

	return e.onEvent(deliveryEventSuccessName)
}

func (e *envelope) onFailure() chan struct{} {
	if e.isClosed() {
		out := make(chan struct{}, 1)

		if e.isClosedWith(deliveryEventFailureName) {
			out <- struct{}{}
		}

		return out
	}

	return e.onEvent(deliveryEventFailureName)
}

func (e *envelope) closeWith(event *deliveryEvent) {
	if e.isClosed() {
		return
	}

	e.setClosedWith(event)

	go func() {
		e.events <- event
		close(e.events)
	}()
}

func (e *envelope) doWithClosedWith(do func() interface{}) interface{} {
	e.closedWithLock.Lock()
	defer e.closedWithLock.Unlock()

	return do()
}

func (e *envelope) isClosed() bool {
	return e.doWithClosedWith(func() interface{} {
		return e.closedWith != nil
	}).(bool)
}

func (e *envelope) isClosedWith(name string) bool {
	return e.doWithClosedWith(func() interface{} {
		return e.closedWith != nil && e.closedWith.name == name
	}).(bool)
}

func (e *envelope) setClosedWith(event *deliveryEvent) {
	e.doWithClosedWith(func() interface{} {
		e.closedWith = event
		return nil
	})
}

func (e *envelope) doWithProxies(do func() interface{}) interface{} {
	e.proxiesLock.Lock()
	defer e.proxiesLock.Unlock()

	return do()
}

func (e *envelope) getProxies() []chan *deliveryEvent {
	return e.doWithProxies(func() interface{} {
		return e.proxies
	}).([]chan *deliveryEvent)
}

func (e *envelope) addProxy(proxy chan *deliveryEvent) {
	e.doWithProxies(func() interface{} {
		e.proxies = append(e.proxies, proxy)
		return nil
	})
}

func newEnvelope(ctx context.Context, batch []*Message) *envelope {
	env := &envelope{
		ctx:   ctx,
		batch: batch,

		events:  make(chan *deliveryEvent),
		proxies: []chan *deliveryEvent{},

		closedWith: nil,
	}

	go func() {
		for event := range env.events {
			for _, proxy := range env.getProxies() {
				proxy <- event
			}
		}

		for _, proxy := range env.getProxies() {
			close(proxy)
		}
	}()

	return env
}
