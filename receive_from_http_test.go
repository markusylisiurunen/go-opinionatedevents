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
		httpMethod     string
		expectedStatus int
	}{
		{
			name:           "a valid request",
			messageData:    `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			httpMethod:     "POST",
			expectedStatus: 200,
		},
		{
			name:           "an invalid payload",
			messageData:    `{"name":"test","meta":{"timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			httpMethod:     "POST",
			expectedStatus: 500,
		},
		{
			name:           "an invalid method",
			messageData:    `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			httpMethod:     "PUT",
			expectedStatus: 404,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.httpMethod, "/events", strings.NewReader(tc.messageData))
			resp := httptest.NewRecorder()

			receiver, err := NewReceiver()
			assert.NoError(t, err)

			MakeReceiveFromHTTP(receiver)(resp, req)

			assert.Equal(t, tc.expectedStatus, resp.Code)
		})
	}
}
