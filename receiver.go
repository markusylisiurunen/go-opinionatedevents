package opinionatedevents

import "fmt"

type OnMessageHandler func(msg *Message) error

type Receiver struct {
	onMessage map[string]OnMessageHandler
}

func (r *Receiver) Receive(data []byte) error {
	msg, err := ParseMessage(data)
	if err != nil {
		return err
	}

	if onMessageHandler, ok := r.onMessage[msg.name]; ok {
		return onMessageHandler(msg)
	}

	return nil
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
