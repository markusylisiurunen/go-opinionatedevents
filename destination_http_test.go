package opinionatedevents

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHTTPDestination(t *testing.T) {
	t.Run("fails if no HTTP handlers added", func(t *testing.T) {
		destination := NewHttpDestination("https://api.example.com/events")
		client := &testHttpClient{}

		destination.client = client

		err := destination.deliver(NewMessage("test"))
		assert.Error(t, err)
	})

	t.Run("successfully delivers a message if endpoint responds with 2xx", func(t *testing.T) {
		destination := NewHttpDestination("https://api.example.com/events")
		client := &testHttpClient{}

		destination.client = client

		i := 0
		client.pushHandler(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "/events", req.URL.Path)

			i += 1

			return &http.Response{StatusCode: 200}, nil
		})

		err := destination.deliver(NewMessage("test"))
		assert.NoError(t, err)

		assert.Equal(t, 1, i)
	})

	t.Run("fails delivering a message if endpoint responds with non-2xx", func(t *testing.T) {
		destination := NewHttpDestination("https://api.example.com/events")
		client := &testHttpClient{}

		destination.client = client

		i := 0
		client.pushHandler(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "/events", req.URL.Path)

			i += 1

			return &http.Response{StatusCode: 404}, nil
		})

		err := destination.deliver(NewMessage("test"))
		assert.Error(t, err)

		assert.Equal(t, 1, i)
	})

	t.Run("sends the message as JSON in the POST body", func(t *testing.T) {
		destination := NewHttpDestination("https://api.example.com/events")
		client := &testHttpClient{}

		destination.client = client

		msg := NewMessage("test")

		err := msg.SetPayload(&testHTTPClientPayload{})
		assert.NoError(t, err)

		client.pushHandler(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, "POST", req.Method)
			assert.Equal(t, "/events", req.URL.Path)

			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))

			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}

			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				return nil, err
			}

			meta, ok := payload["meta"].(map[string]interface{})
			assert.True(t, ok)

			assert.IsType(t, "", payload["name"])
			assert.IsType(t, "", payload["payload"])
			assert.IsType(t, "", meta["timestamp"])

			assert.Equal(t, "test", payload["name"])
			assert.Equal(t, msg.meta.timestamp.Format(time.RFC3339Nano), meta["timestamp"])

			payloadAsJson, err := base64.StdEncoding.DecodeString(payload["payload"].(string))
			assert.NoError(t, err)

			var data map[string]interface{}
			assert.NoError(t, json.Unmarshal(payloadAsJson, &data))

			assert.Equal(t, "world", data["hello"])
			assert.Equal(t, true, data["ok"])
			assert.Equal(t, 4.0, data["age"])

			return &http.Response{StatusCode: 200}, nil
		})

		err := destination.deliver(msg)
		assert.NoError(t, err)
	})
}

type testHTTPClientPayload struct{}

func (p *testHTTPClientPayload) MarshalPayload() ([]byte, error) {
	payload := map[string]interface{}{"hello": "world", "ok": true, "age": 4}
	return json.Marshal(payload)
}

type testHttpClient struct {
	handlers []func(req *http.Request) (*http.Response, error)
}

func (c *testHttpClient) Do(req *http.Request) (*http.Response, error) {
	if len(c.handlers) == 0 {
		return nil, fmt.Errorf("no handlers left")
	}

	handler := c.handlers[0]
	c.handlers = c.handlers[1:]

	return handler(req)
}

func (c *testHttpClient) pushHandler(handler func(req *http.Request) (*http.Response, error)) {
	c.handlers = append(c.handlers, handler)
}
