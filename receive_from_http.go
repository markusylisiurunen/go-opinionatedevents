package opinionatedevents

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// TODO: return the retry information in some consistent way
// TODO: somehow extract the delivery attempt information from headers
func MakeReceiveFromHTTP(_ context.Context, receiver *Receiver) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			resp.WriteHeader(404)
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			return
		}

		delivery, err := newHTTPDelivery(body, 1)
		if err != nil {
			resp.WriteHeader(500)
			return
		}

		if result := receiver.Receive(req.Context(), "test", delivery); result.GetResult().Err != nil {
			resp.WriteHeader(500)
		}
	}
}

type httpDelivery struct {
	attempt int
	message *Message
}

func newHTTPDelivery(data []byte, attempt int) (*httpDelivery, error) {
	msg := &Message{}
	if err := json.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return &httpDelivery{attempt: attempt, message: msg}, nil
}

func (d *httpDelivery) GetAttempt() int {
	return d.attempt
}

func (d *httpDelivery) GetMessage() *Message {
	return d.message
}
