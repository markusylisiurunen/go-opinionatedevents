package opinionatedevents

import (
	"errors"
)

type PubSubDestination struct{}

func (d *PubSubDestination) deliver(m *Message) error {
	return errors.New("not implemented")
}

func NewPubSubDestination() *PubSubDestination {
	return &PubSubDestination{}
}
