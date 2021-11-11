package opinionatedevents

import (
	"io/ioutil"
	"net/http"
)

type ReceiveFromHTTP func(resp http.ResponseWriter, req *http.Request)

func MakeReceiveFromHTTP(receiver *Receiver) ReceiveFromHTTP {
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

		if err := receiver.Receive(body); err != nil {
			resp.WriteHeader(500)
		}
	}
}
