package opinionatedevents

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHTTPDestination(t *testing.T) {
	t.Run("fails if no HTTP handlers added", func(t *testing.T) {
		ctx := context.Background()
		destination := NewHTTPDestination("https://api.example.com/events")
		client := &testHTTPClient{}
		destination.setClient(client)
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		err = destination.Deliver(ctx, msg)
		assert.Error(t, err)
	})

	t.Run("successfully delivers a message if endpoint responds with 200", func(t *testing.T) {
		ctx := context.Background()
		destination := NewHTTPDestination("https://api.example.com/events")
		client := &testHTTPClient{}
		destination.setClient(client)
		// configure the handler
		i := 0
		client.pushHandler(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "/events", req.URL.Path)
			i += 1
			return &http.Response{StatusCode: 200}, nil
		})
		// publish the message
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		err = destination.Deliver(ctx, msg)
		assert.NoError(t, err)
		assert.Equal(t, 1, i)
	})

	t.Run("fails delivering a message if endpoint responds with non-200", func(t *testing.T) {
		ctx := context.Background()
		destination := NewHTTPDestination("https://api.example.com/events")
		client := &testHTTPClient{}
		destination.setClient(client)
		// configure the handler
		i := 0
		client.pushHandler(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "/events", req.URL.Path)
			i += 1
			return &http.Response{StatusCode: 404}, nil
		})
		// publish the message
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		err = destination.Deliver(ctx, msg)
		assert.Error(t, err)
		assert.Equal(t, 1, i)
	})

	t.Run("sends the message as JSON in the POST body", func(t *testing.T) {
		ctx := context.Background()
		destination := NewHTTPDestination("https://api.example.com/events")
		client := &testHTTPClient{}
		destination.setClient(client)
		// create the message
		msg, err := NewMessage("test.test", &testHTTPClientPayload{})
		assert.NoError(t, err)
		// configure the handler
		i := 0
		client.pushHandler(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "/events", req.URL.Path)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			i += 1
			// parse the request payload
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				return nil, err
			}
			meta, ok := payload["meta"].(map[string]interface{})
			assert.True(t, ok)
			// assert the data types
			assert.IsType(t, "", payload["name"])
			assert.IsType(t, "", payload["payload"])
			assert.IsType(t, "", meta["published_at"])
			// assert some fields
			assert.Equal(t, "test.test", payload["name"])
			assert.Equal(t, msg.GetPublishedAt().UTC().Format(time.RFC3339Nano), meta["published_at"])
			// parse the message payload
			payloadAsJson, err := base64.StdEncoding.DecodeString(payload["payload"].(string))
			assert.NoError(t, err)
			var data map[string]interface{}
			assert.NoError(t, json.Unmarshal(payloadAsJson, &data))
			assert.Equal(t, "world", data["hello"])
			assert.Equal(t, true, data["ok"])
			assert.Equal(t, 4.0, data["age"])
			return &http.Response{StatusCode: 200}, nil
		})
		// publish the message
		deliveryErr := destination.Deliver(ctx, msg)
		assert.NoError(t, deliveryErr)
	})
}

type testHTTPClientPayload struct{}

func (p *testHTTPClientPayload) MarshalJSON() ([]byte, error) {
	payload := map[string]interface{}{"hello": "world", "ok": true, "age": 4}
	return json.Marshal(payload)
}

type testHTTPClient struct {
	handlers []func(req *http.Request) (*http.Response, error)
}

func (c *testHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if len(c.handlers) == 0 {
		return nil, fmt.Errorf("no handlers left")
	}
	handler := c.handlers[0]
	c.handlers = c.handlers[1:]
	return handler(req)
}

func (c *testHTTPClient) pushHandler(handler func(req *http.Request) (*http.Response, error)) {
	c.handlers = append(c.handlers, handler)
}
