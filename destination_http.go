package opinionatedevents

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type HttpDestination struct {
	endpoint string
	client   httpClient
}

func (d *HttpDestination) Deliver(msg *Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, d.endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}

	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !ok {
		return fmt.Errorf("endpoint returned a %d status", resp.StatusCode)
	}

	return nil
}

func NewHttpDestination(endpoint string) *HttpDestination {
	return &HttpDestination{
		endpoint: endpoint,
		client:   http.DefaultClient,
	}
}
