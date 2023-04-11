package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
)

type Delivery interface {
	GetAttempt() int
	GetMessage() *Message
}

type OnMessageHandler func(ctx context.Context, queue string, delivery Delivery) ResultContainer

type Receiver struct {
	onMessage map[string]map[string]OnMessageHandler
}

func (r *Receiver) Receive(ctx context.Context, queue string, delivery Delivery) ResultContainer {
	msg := delivery.GetMessage()
	if onMessageForQueue, ok := r.onMessage[queue]; ok {
		if onMessageHandler, ok := onMessageForQueue[msg.name]; ok {
			return onMessageHandler(ctx, queue, delivery)
		}
	}
	return FatalResult(errors.New("no message handler found"))
}

func (r *Receiver) On(queue string, name string, onMessage OnMessageHandler) error {
	if _, ok := r.onMessage[queue]; !ok {
		r.onMessage[queue] = map[string]OnMessageHandler{}
	}
	if _, ok := r.onMessage[queue][name]; ok {
		return fmt.Errorf("only one handler per message type is allowed")
	}
	r.onMessage[queue][name] = onMessage
	return nil
}

func NewReceiver() (*Receiver, error) {
	receiver := &Receiver{
		onMessage: map[string]map[string]OnMessageHandler{},
	}
	return receiver, nil
}
