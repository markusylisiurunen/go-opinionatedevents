package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
)

type OnMessageHandler func(ctx context.Context, queue string, msg *Message) Result

type Delivery struct {
	Data    []byte
	Queue   string
	Attempt int
}

type Receiver struct {
	onMessage map[string]map[string]OnMessageHandler
}

func (r *Receiver) Receive(ctx context.Context, delivery Delivery) Result {
	msg, err := newMessageFromSendable(delivery.Data, messageDeliveryMeta{attempt: delivery.Attempt})
	if err != nil {
		return ErrorResult(err)
	}

	if onMessageForQueue, ok := r.onMessage[delivery.Queue]; ok {
		if onMessageHandler, ok := onMessageForQueue[msg.name]; ok {
			return onMessageHandler(ctx, delivery.Queue, msg)
		}
	}

	// if there is no handler attached, the message will be dropped
	// TODO: is this correct behavior? should there be some kind of a fallback handler?
	return ErrorResult(errors.New("no message handler found"),
		ResultWithNoRetries(),
	)
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
