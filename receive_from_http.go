package opinionatedevents

import (
	"context"
	"io/ioutil"
	"net/http"
)

func MakeReceiveFromHTTP(_ context.Context, receiver *Receiver) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			resp.WriteHeader(404)
			return
		}

		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			resp.WriteHeader(500)
			return
		}

		// TODO: handle the result properly (e.g., retry delay)
		if result := receiver.Receive(req.Context(), body); result.error() != nil {
			resp.WriteHeader(500)
		}
	}
}
