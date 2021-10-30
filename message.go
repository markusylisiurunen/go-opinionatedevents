package opinionatedevents

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type sendableMessageMeta struct {
	UUID      string    `json:"uuid"`
	Timestamp time.Time `json:"timestamp"`
}

type sendableMessage struct {
	Name    string              `json:"name"`
	Payload []byte              `json:"payload"`
	Meta    sendableMessageMeta `json:"meta"`
}

type Payloadable interface {
	MarshalPayload() ([]byte, error)
}

type Message struct {
	name    string
	payload []byte

	meta struct {
		uuid      string
		timestamp time.Time
	}
}

func (msg *Message) SetPayload(payload Payloadable) error {
	data, err := payload.MarshalPayload()
	if err != nil {
		return err
	}

	msg.payload = data

	return nil
}

func (msg *Message) MarshalJSON() ([]byte, error) {
	tmp := &sendableMessage{
		Name:    msg.name,
		Payload: msg.payload,

		Meta: sendableMessageMeta{
			UUID:      msg.meta.uuid,
			Timestamp: msg.meta.timestamp.UTC(),
		},
	}

	return json.Marshal(tmp)
}

func NewMessage(name string) *Message {
	return &Message{
		name:    name,
		payload: nil,

		meta: struct {
			uuid      string
			timestamp time.Time
		}{
			uuid:      uuid.New().String(),
			timestamp: time.Now(),
		},
	}
}
