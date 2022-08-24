package opinionatedevents

import (
	"context"
	"errors"
	"fmt"
)

type OnMessageHandler func(ctx context.Context, msg *Message) Result

type Delivery struct {
	Data    []byte
	Attempt int
}

type Receiver struct {
	onMessage map[string]OnMessageHandler
}

func (r *Receiver) Receive(ctx context.Context, delivery Delivery) Result {
	msg, err := newMessageFromSendable(delivery.Data, messageDeliveryMeta{attempt: delivery.Attempt})
	if err != nil {
		return ErrorResult(err)
	}

	if onMessageHandler, ok := r.onMessage[msg.name]; ok {
		return onMessageHandler(ctx, msg)
	}

	// if there is no handler attached, the message will be dropped
	return ErrorResult(errors.New("no message handler found"),
		ResultWithNoRetries(),
	)
}

func (r *Receiver) On(name string, onMessage OnMessageHandler) error {
	if _, ok := r.onMessage[name]; ok {
		return fmt.Errorf("only one handler per message type is allowed")
	}

	r.onMessage[name] = onMessage

	return nil
}

func NewReceiver() (*Receiver, error) {
	receiver := &Receiver{
		onMessage: map[string]OnMessageHandler{},
	}

	return receiver, nil
}
