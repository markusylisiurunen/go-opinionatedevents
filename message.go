package opinionatedevents

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type encodedMessageMeta struct {
	PublishedAt time.Time `json:"published_at" validate:"required"`
	UUID        string    `json:"uuid" validate:"required"`
}

type encodedMessage struct {
	Meta    encodedMessageMeta `json:"meta" validate:"required"`
	Name    string             `json:"name" validate:"required"`
	Payload []byte             `json:"payload"`
}

type payloadable interface {
	MarshalPayload() ([]byte, error)
	UnmarshalPayload([]byte) error
}

type Message struct {
	Name        string
	PublishedAt time.Time
	UUID        string

	payload []byte
}

func (msg *Message) Payload(payload payloadable) error {
	return payload.UnmarshalPayload(msg.payload)
}

var validate *validator.Validate

func getValidator() *validator.Validate {
	if validate == nil {
		validate = validator.New()
	}
	return validate
}

func (msg *Message) MarshalJSON() ([]byte, error) {
	s := encodedMessage{
		Meta:    encodedMessageMeta{PublishedAt: msg.PublishedAt.UTC(), UUID: msg.UUID},
		Name:    msg.Name,
		Payload: msg.payload,
	}
	if err := getValidator().Struct(&s); err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

func (msg *Message) UnmarshalJSON(data []byte) error {
	var s encodedMessage
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if err := getValidator().Struct(&s); err != nil {
		return err
	}
	msg.Name = s.Name
	msg.PublishedAt = s.Meta.PublishedAt
	msg.UUID = s.Meta.UUID
	msg.payload = s.Payload
	return nil
}

func NewMessage(name string, payload payloadable) (*Message, error) {
	pattern := "^[a-zA-Z0-9_\\-]+\\.[a-zA-Z0-9_\\-]+$"
	if matched, _ := regexp.MatchString(pattern, name); !matched {
		return nil, fmt.Errorf("name must match the pattern: %s", pattern)
	}
	msg := &Message{
		Name:        name,
		PublishedAt: time.Now(),
		UUID:        uuid.NewString(),
	}
	if payload != nil {
		data, err := payload.MarshalPayload()
		if err != nil {
			return nil, err
		}
		msg.payload = data
	}
	return msg, nil
}
