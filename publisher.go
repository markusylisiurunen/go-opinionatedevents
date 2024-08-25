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
	handler func(batch []*Message)
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
	publisher := &Publisher{
		bridge:                    nil,
		inFlightWaitingGroup:      sync.WaitGroup{},
		onDeliveryFailureHandlers: []*onDeliveryFailureHandler{},
	}
	for _, apply := range opts {
		if err := apply(publisher); err != nil {
			return nil, err
		}
	}
	if publisher.bridge == nil {
		return nil, errors.New("publisher bridge was not configured")
	}
	return publisher, nil
}

func (p *Publisher) OnDeliveryFailure(handler func(batch []*Message)) func() {
	p.onDeliveryFailureHandlers = append(p.onDeliveryFailureHandlers,
		&onDeliveryFailureHandler{handler: handler},
	)
	return func() {
		for i := 0; i < len(p.onDeliveryFailureHandlers); i += 1 {
			if reflect.ValueOf(p.onDeliveryFailureHandlers[i].handler).Pointer() == reflect.ValueOf(handler).Pointer() {
				p.onDeliveryFailureHandlers = append(p.onDeliveryFailureHandlers[:i], p.onDeliveryFailureHandlers[i+1:]...)
				break
			}
		}
	}
}

func (p *Publisher) PublishOne(ctx context.Context, msg *Message) error {
	batch := []*Message{msg}
	return p.publish(ctx, batch)
}

func (p *Publisher) PublishMany(ctx context.Context, batch []*Message) error {
	return p.publish(ctx, batch)
}

func (p *Publisher) publish(ctx context.Context, batch []*Message) error {
	for _, msg := range batch {
		if msg.publishedAt.IsZero() {
			msg.publishedAt = time.Now()
		}
		if msg.deliverAt.IsZero() {
			msg.deliverAt = msg.publishedAt
		}
	}
	p.inFlightWaitingGroup.Add(1)
	envelope := p.bridge.take(ctx, batch)
	// FIXME: this entire `isClosed` is fucked up, eg. there is no way to extract the actual error...
	if envelope.isClosed() {
		// the envelope was closed synchronously -> handle result synchronously
		p.inFlightWaitingGroup.Done()
		if envelope.isClosedWith(deliveryEventFailureName) {
			for _, handleFailure := range p.onDeliveryFailureHandlers {
				handleFailure.handler(batch)
			}
			return fmt.Errorf("error publishing a batch of messages: %#v", batch)
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
				handleFailure.handler(batch)
			}
		}
	}()
	return nil
}

func (p *Publisher) Drain() {
	p.inFlightWaitingGroup.Wait()
}
