package opinionatedevents

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReceiver(t *testing.T) {
	log := []string{}

	tt := []struct {
		name               string
		messageData        string
		logAfterReceive    []string
		errorsAfterReceive bool
		onMessageHandlers  map[string]OnMessageHandler
	}{
		{
			name:               "a valid test message",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{"test"},
			errorsAfterReceive: false,

			onMessageHandlers: map[string]OnMessageHandler{
				"test":    makeOnMessageHandler("test", &log, false),
				"unknown": makeOnMessageHandler("unknown", &log, false),
			},
		},
		{
			name:               "no handler registered",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{},
			errorsAfterReceive: true,

			onMessageHandlers: map[string]OnMessageHandler{
				"unknown": makeOnMessageHandler("unknown", &log, false),
			},
		},
		{
			name:               "an invalid message",
			messageData:        `{"name":"test","meta":{"timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{},
			errorsAfterReceive: true,

			onMessageHandlers: map[string]OnMessageHandler{
				"test": makeOnMessageHandler("test", &log, false),
			},
		},
		{
			name:               "handler returns an error",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{"test"},
			errorsAfterReceive: true,

			onMessageHandlers: map[string]OnMessageHandler{
				"test": makeOnMessageHandler("test", &log, true),
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			log = []string{}

			receiver, err := NewReceiver()
			assert.NoError(t, err)

			for name, onMessageHandler := range tc.onMessageHandlers {
				assert.NoError(t, receiver.On(name, onMessageHandler))
			}

			result := receiver.Receive(context.Background(), Delivery{[]byte(tc.messageData), 1})

			if tc.errorsAfterReceive {
				assert.Error(t, result.error())
			} else {
				assert.NoError(t, result.error())
			}

			assert.Len(t, log, len(tc.logAfterReceive))
			assert.Equal(t, tc.logAfterReceive, log)
		})
	}
}

func makeOnMessageHandler(name string, log *[]string, returnsErr bool) OnMessageHandler {
	return func(_ context.Context, _ *Message) Result {
		*log = append(*log, name)

		if returnsErr {
			return ErrorResult(errors.New("it failed"))
		}

		return SuccessResult()
	}
}
