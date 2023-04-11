package opinionatedevents

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type encodedMeta struct {
	UUID        string    `json:"uuid" validate:"required"`
	PublishedAt time.Time `json:"published_at" validate:"required"`
}

type encodedMessage struct {
	Name    string      `json:"name" validate:"required"`
	Meta    encodedMeta `json:"meta" validate:"required"`
	Payload []byte      `json:"payload"`
}

type Message struct {
	uuid        string
	name        string
	publishedAt time.Time
	payload     []byte
}

func (msg *Message) GetUUID() string {
	return msg.uuid
}

func (msg *Message) GetName() string {
	return msg.name
}

func (msg *Message) GetTopic() string {
	return strings.Split(msg.name, ".")[0]
}

func (msg *Message) GetPublishedAt() time.Time {
	return msg.publishedAt
}

func (msg *Message) GetPayload(payload any) error {
	return json.Unmarshal(msg.payload, payload)
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
		Name:    msg.name,
		Meta:    encodedMeta{UUID: msg.uuid, PublishedAt: msg.publishedAt.UTC()},
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
	msg.uuid = s.Meta.UUID
	msg.name = s.Name
	msg.publishedAt = s.Meta.PublishedAt
	msg.payload = s.Payload
	return nil
}

func NewMessage(name string, payload any) (*Message, error) {
	pattern := "^[a-zA-Z0-9_\\-]+\\.[a-zA-Z0-9_\\-]+$"
	if matched, _ := regexp.MatchString(pattern, name); !matched {
		return nil, fmt.Errorf("name must match the pattern: %s", pattern)
	}
	msg := &Message{
		uuid:        uuid.NewString(),
		name:        name,
		publishedAt: time.Now(),
	}
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		msg.payload = data
	}
	return msg, nil
}
