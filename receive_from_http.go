package opinionatedevents

import (
	"context"
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
			resp.WriteHeader(500)
			return
		}

		if result := receiver.Receive(req.Context(), Delivery{body, 1}); result.error() != nil {
			resp.WriteHeader(500)
		}
	}
}
