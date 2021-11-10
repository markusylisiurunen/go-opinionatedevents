package opinionatedevents

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReceiveFromHTTP(t *testing.T) {
	tt := []struct {
		name           string
		messageData    string
		expectedStatus int
	}{
		{
			name:           "a valid request",
			messageData:    `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			expectedStatus: 200,
		},
		{
			name:           "an invalid payload",
			messageData:    `{"name":"test","meta":{"timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			expectedStatus: 500,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/events", strings.NewReader(tc.messageData))
			resp := httptest.NewRecorder()

			receiver, err := NewReceiver()
			assert.NoError(t, err)

			MakeReceiveFromHttp(receiver)(resp, req)

			assert.Equal(t, tc.expectedStatus, resp.Code)
		})
	}
}
