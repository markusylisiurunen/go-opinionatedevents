package opinionatedevents

import (
	"io/ioutil"
	"net/http"
)

type ReceiveFromHttp func(resp http.ResponseWriter, req *http.Request)

func MakeReceiveFromHttp(receiver *Receiver) ReceiveFromHttp {
	return func(resp http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			resp.WriteHeader(500)
			return
		}

		if err := receiver.Receive(body); err != nil {
			resp.WriteHeader(500)
			return
		}
	}
}
