package opinionatedevents

import (
	"encoding/json"
	"fmt"
)

type HttpDestination struct {
	endpoints []string
}

func (d *HttpDestination) deliver(m *Message) error {
	// TODO: make the actual HTTP requests here...
	for _, endpoint := range d.endpoints {
		fmt.Printf("sending to %s\n", endpoint)

		d, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}

		fmt.Printf("%s\n", d)
	}

	return nil
}

func (d *HttpDestination) AddEndpoint(endpoint string) {
	d.endpoints = append(d.endpoints, endpoint)
}

func NewHttpDestination() *HttpDestination {
	return &HttpDestination{}
}
