package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
)

type Delivery interface {
	GetAttempt() int
	GetQueue() string
	GetMessage() *Message
}

type OnMessageHandler func(ctx context.Context, delivery Delivery) error

type Receiver struct {
	started   bool
	sources   []Source
	onMessage map[string]map[string]OnMessageHandler
}

type receiverOption func(r *Receiver) error

func ReceiverWithSource(source Source) receiverOption {
	return func(r *Receiver) error {
		r.sources = append(r.sources, source)
		return nil
	}
}

func NewReceiver(opts ...receiverOption) (*Receiver, error) {
	receiver := &Receiver{
		started:   false,
		sources:   []Source{},
		onMessage: map[string]map[string]OnMessageHandler{},
	}
	for _, apply := range opts {
		if err := apply(receiver); err != nil {
			return nil, err
		}
	}
	return receiver, nil
}

func (r *Receiver) Start(ctx context.Context) error {
	if r.started {
		return errors.New("cannot start the receiver more than once")
	}
	r.started = true
	for _, source := range r.sources {
		if err := source.Start(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (r *Receiver) GetQueuesWithHandlers() []string {
	result := []string{}
	for queue := range r.onMessage {
		result = append(result, queue)
	}
	return result
}

func (r *Receiver) GetMessagesWithHandlers(queue string) []string {
	result := []string{}
	if onMessageForQueue, ok := r.onMessage[queue]; ok {
		for name := range onMessageForQueue {
			result = append(result, name)
		}
	}
	return result
}

func (r *Receiver) Deliver(ctx context.Context, delivery Delivery) error {
	if !r.started {
		panic(fmt.Errorf(`an unexpected delivery before the receiver was started`))
	}
	queue, msg := delivery.GetQueue(), delivery.GetMessage()
	if onMessageForQueue, ok := r.onMessage[queue]; ok {
		if onMessageHandler, ok := onMessageForQueue[msg.name]; ok {
			return onMessageHandler(ctx, delivery)
		}
	}
	err := fmt.Errorf(
		`an unexpected delivery of message "%s" from queue "%s" with no handler defined`,
		msg.GetName(), queue,
	)
	panic(err)
}

func (r *Receiver) On(queue string, name string, onMessage OnMessageHandler) error {
	if _, ok := r.onMessage[queue]; !ok {
		r.onMessage[queue] = map[string]OnMessageHandler{}
	}
	if _, ok := r.onMessage[queue][name]; ok {
		return fmt.Errorf("only one handler per queue per message is allowed")
	}
	r.onMessage[queue][name] = onMessage
	return nil
}
