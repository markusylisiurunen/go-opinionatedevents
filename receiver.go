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
	onMessage map[string]map[string]OnMessageHandler
}

func NewReceiver() (*Receiver, error) {
	receiver := &Receiver{onMessage: map[string]map[string]OnMessageHandler{}}
	return receiver, nil
}

func (r *Receiver) Deliver(ctx context.Context, delivery Delivery) error {
	queue, msg := delivery.GetQueue(), delivery.GetMessage()
	if onMessageForQueue, ok := r.onMessage[queue]; ok {
		if onMessageHandler, ok := onMessageForQueue[msg.name]; ok {
			return onMessageHandler(ctx, delivery)
		}
	}
	// FIXME: definitely should not error messages that do not have a handler defined
	return Fatal(errors.New("no message handler found"))
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
