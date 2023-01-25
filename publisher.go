package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type Publisher struct {
	bridge                    bridge
	inFlightWaitingGroup      sync.WaitGroup
	onDeliveryFailureHandlers []func(msg *Message)
}

func (p *Publisher) OnDeliveryFailure(handler func(msg *Message)) func() {
	p.onDeliveryFailureHandlers = append(p.onDeliveryFailureHandlers, handler)

	return func() {
		for i := 0; i < len(p.onDeliveryFailureHandlers); i += 1 {
			if reflect.ValueOf(p.onDeliveryFailureHandlers[i]).Pointer() == reflect.ValueOf(handler).Pointer() {
				p.onDeliveryFailureHandlers = append(p.onDeliveryFailureHandlers[:i], p.onDeliveryFailureHandlers[i+1:]...)
			}
		}
	}
}

func (p *Publisher) Publish(ctx context.Context, msg *Message) error {
	if msg.PublishedAt.IsZero() {
		msg.PublishedAt = time.Now()
	}
	p.inFlightWaitingGroup.Add(1)
	envelope := p.bridge.take(ctx, msg)
	if envelope.isClosed() {
		// the envelope was closed synchronously -> handle result synchronously
		p.inFlightWaitingGroup.Done()
		if envelope.isClosedWith(deliveryEventFailureName) {
			for _, handleFailure := range p.onDeliveryFailureHandlers {
				handleFailure(msg)
			}
			return fmt.Errorf("error publishing a message: %#v", msg)
		}
		return nil
	}
	// the envelope will be closed asynchronously -> return nil error
	go func() {
		select {
		case <-envelope.onSuccess():
			p.inFlightWaitingGroup.Done()
		case <-envelope.onFailure():
			p.inFlightWaitingGroup.Done()
			for _, handleFailure := range p.onDeliveryFailureHandlers {
				handleFailure(msg)
			}
		}
	}()
	return nil
}

func (p *Publisher) Drain() {
	p.inFlightWaitingGroup.Wait()
}

func NewPublisher(opts ...PublisherOption) (*Publisher, error) {
	p := &Publisher{
		onDeliveryFailureHandlers: []func(msg *Message){},
	}

	for _, modify := range opts {
		if err := modify(p); err != nil {
			return nil, err
		}
	}

	if p.bridge == nil {
		return nil, errors.New("bridge was not configured properly")
	}

	return p, nil
}

type PublisherOption func(p *Publisher) error

func PublisherWithSyncBridge(destinations ...Destination) PublisherOption {
	return func(p *Publisher) error {
		if p.bridge != nil {
			return errors.New("cannot initialise bridge more than once")
		}

		p.bridge = newSyncBridge(destinations...)

		return nil
	}
}

func PublisherWithAsyncBridge(
	maxAttempts int,
	waitBetweenAttempts int,
	destinations ...Destination,
) PublisherOption {
	return func(p *Publisher) error {
		if p.bridge != nil {
			return errors.New("cannot initialise bridge more than once")
		}

		p.bridge = newAsyncBridge(maxAttempts, waitBetweenAttempts, destinations...)

		return nil
	}
}
