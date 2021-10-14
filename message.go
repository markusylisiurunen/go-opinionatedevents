package opinionatedevents

import (
	"encoding/json"
	"time"
)

type sendableMessageMeta struct {
	Timestamp time.Time `json:"timestamp"`
}

type sendableMessage struct {
	Name    string               `json:"name"`
	Payload []byte               `json:"payload"`
	Meta    *sendableMessageMeta `json:"meta"`
}

type Payloadable interface {
	MarshalPayload() ([]byte, error)
}

type Message struct {
	name    string
	payload []byte

	meta struct {
		timestamp time.Time
	}
}

func (m *Message) SetPayload(payload Payloadable) error {
	data, err := payload.MarshalPayload()
	if err != nil {
		return err
	}

	m.payload = data

	return nil
}

func (m *Message) MarshalJSON() ([]byte, error) {
	tmp := &sendableMessage{
		Name:    m.name,
		Payload: m.payload,
		Meta: &sendableMessageMeta{
			Timestamp: m.meta.timestamp.UTC(),
		},
	}

	return json.Marshal(tmp)
}

func NewMessage(name string) *Message {
	return &Message{
		name:    name,
		payload: nil,
		meta:    struct{ timestamp time.Time }{},
	}
}
