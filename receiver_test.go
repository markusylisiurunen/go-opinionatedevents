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
		name                  string
		messageQueue          string
		messageData           string
		logAfterReceive       []string
		errorsAfterReceive    bool
		onMessageHandlerQueue string
		onMessageHandlers     map[string]OnMessageHandler
	}{
		{
			name:               "a valid test message",
			messageQueue:       "test",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{"test"},
			errorsAfterReceive: false,

			onMessageHandlerQueue: "test",
			onMessageHandlers: map[string]OnMessageHandler{
				"test":    makeOnMessageHandler("test", &log, false),
				"unknown": makeOnMessageHandler("unknown", &log, false),
			},
		},
		{
			name:               "no handler registered for message name",
			messageQueue:       "test",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{},
			errorsAfterReceive: true,

			onMessageHandlerQueue: "test",
			onMessageHandlers: map[string]OnMessageHandler{
				"unknown": makeOnMessageHandler("unknown", &log, false),
			},
		},
		{
			name:               "no handler registered for queue",
			messageQueue:       "test",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{},
			errorsAfterReceive: true,

			onMessageHandlerQueue: "unknown",
			onMessageHandlers: map[string]OnMessageHandler{
				"test": makeOnMessageHandler("test", &log, false),
			},
		},
		{
			name:               "an invalid message",
			messageQueue:       "test",
			messageData:        `{"name":"test","meta":{"timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{},
			errorsAfterReceive: true,

			onMessageHandlerQueue: "test",
			onMessageHandlers: map[string]OnMessageHandler{
				"test": makeOnMessageHandler("test", &log, false),
			},
		},
		{
			name:               "handler returns an error",
			messageQueue:       "test",
			messageData:        `{"name":"test","meta":{"uuid":"12345","timestamp":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:    []string{"test"},
			errorsAfterReceive: true,

			onMessageHandlerQueue: "test",
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
				assert.NoError(t, receiver.On(name, tc.onMessageHandlerQueue, onMessageHandler))
			}

			result := receiver.Receive(context.Background(), Delivery{
				Data:    []byte(tc.messageData),
				Queue:   tc.messageQueue,
				Attempt: 1,
			})

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
	return func(_ context.Context, _ string, _ *Message) Result {
		*log = append(*log, name)

		if returnsErr {
			return ErrorResult(errors.New("it failed"))
		}

		return SuccessResult()
	}
}
