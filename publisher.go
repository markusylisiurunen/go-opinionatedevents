package opinionatedevents

import (
	"errors"
	"time"
)

type Publisher struct {
	bridge bridge
}

func (p *Publisher) Publish(m *Message) error {
	m.meta.timestamp = time.Now()
	return p.bridge.take(m)
}

func (p *Publisher) Drain() {
	p.bridge.drain()
}

func NewPublisher(opts ...PublisherOption) (*Publisher, error) {
	p := &Publisher{}

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

func WithSyncBridge(destinations ...destination) PublisherOption {
	return func(p *Publisher) error {
		if p.bridge != nil {
			return errors.New("cannot initialise bridge more than once")
		}

		p.bridge = newSyncBridge(destinations...)

		return nil
	}
}

func WithAsyncBridge(maxBuffer int, maxDelay int, destinations ...destination) PublisherOption {
	return func(p *Publisher) error {
		if p.bridge != nil {
			return errors.New("cannot initialise bridge more than once")
		}

		p.bridge = newAsyncBridge(destinations...)

		return nil
	}
}
