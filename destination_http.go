package opinionatedevents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type httpDestination struct {
	endpoint string
	client   httpClient
}

func NewHTTPDestination(endpoint string) *httpDestination {
	return &httpDestination{
		endpoint: endpoint,
		client:   http.DefaultClient,
	}
}

func (d *httpDestination) setClient(client httpClient) {
	d.client = client
}

func (d *httpDestination) Deliver(_ context.Context, batch []*Message) error {
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	// construct the request
	req, err := http.NewRequest(http.MethodPost, d.endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// make the request
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("endpoint returned a non-200 status code: %d", resp.StatusCode)
	}
	return nil
}
