package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type onDeliveryFailureHandler struct {
	handler func(msg *Message)
}

type Publisher struct {
	bridge                    bridge
	inFlightWaitingGroup      sync.WaitGroup
	onDeliveryFailureHandlers []*onDeliveryFailureHandler
}

type publisherOption func(p *Publisher) error

func PublisherWithSyncBridge(destinations ...Destination) publisherOption {
	return func(p *Publisher) error {
		if p.bridge != nil {
			return errors.New("cannot initialise publisher bridge more than once")
		}
		p.bridge = newSyncBridge(destinations...)
		return nil
	}
}

func PublisherWithAsyncBridge(
	maxAttempts int,
	waitBetweenAttempts int,
	destinations ...Destination,
) publisherOption {
	return func(p *Publisher) error {
		if p.bridge != nil {
			return errors.New("cannot initialise publisher bridge more than once")
		}
		p.bridge = newAsyncBridge(maxAttempts, waitBetweenAttempts, destinations...)
		return nil
	}
}

func NewPublisher(opts ...publisherOption) (*Publisher, error) {
	p := &Publisher{
		bridge:                    nil,
		inFlightWaitingGroup:      sync.WaitGroup{},
		onDeliveryFailureHandlers: []*onDeliveryFailureHandler{},
	}
	for _, apply := range opts {
		if err := apply(p); err != nil {
			return nil, err
		}
	}
	if p.bridge == nil {
		return nil, errors.New("publisher bridge was not configured")
	}
	return p, nil
}

func (p *Publisher) OnDeliveryFailure(handler func(msg *Message)) func() {
	p.onDeliveryFailureHandlers = append(p.onDeliveryFailureHandlers, &onDeliveryFailureHandler{
		handler: handler,
	})
	return func() {
		for i := 0; i < len(p.onDeliveryFailureHandlers); i += 1 {
			if reflect.ValueOf(p.onDeliveryFailureHandlers[i].handler).Pointer() == reflect.ValueOf(handler).Pointer() {
				p.onDeliveryFailureHandlers = append(p.onDeliveryFailureHandlers[:i], p.onDeliveryFailureHandlers[i+1:]...)
				break
			}
		}
	}
}

func (p *Publisher) Publish(ctx context.Context, msg *Message) error {
	if msg.publishedAt.IsZero() {
		msg.publishedAt = time.Now()
	}
	p.inFlightWaitingGroup.Add(1)
	envelope := p.bridge.take(ctx, msg)
	// FIXME: this entire `isClosed` is fucked up, eg. there is no way to extract the actual error...
	if envelope.isClosed() {
		// the envelope was closed synchronously -> handle result synchronously
		p.inFlightWaitingGroup.Done()
		if envelope.isClosedWith(deliveryEventFailureName) {
			for _, handleFailure := range p.onDeliveryFailureHandlers {
				handleFailure.handler(msg)
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
				handleFailure.handler(msg)
			}
		}
	}()
	return nil
}

func (p *Publisher) Drain() {
	p.inFlightWaitingGroup.Wait()
}
