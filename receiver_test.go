package opinionatedevents

import (
	"context"
	"encoding/json"
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
		errorsAfterUnmarshal  bool
		errorsAfterReceive    bool
		onMessageHandlerQueue string
		onMessageHandlers     map[string]OnMessageHandler
	}{
		{
			name:                 "a valid test message",
			messageQueue:         "test",
			messageData:          `{"name":"test","meta":{"uuid":"12345","published_at":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:      []string{"test"},
			errorsAfterUnmarshal: false,
			errorsAfterReceive:   false,

			onMessageHandlerQueue: "test",
			onMessageHandlers: map[string]OnMessageHandler{
				"test":    makeOnMessageHandler("test", &log, false),
				"unknown": makeOnMessageHandler("unknown", &log, false),
			},
		},
		{
			name:                 "no handler registered for message name",
			messageQueue:         "test",
			messageData:          `{"name":"test","meta":{"uuid":"12345","published_at":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:      []string{},
			errorsAfterUnmarshal: false,
			errorsAfterReceive:   true,

			onMessageHandlerQueue: "test",
			onMessageHandlers: map[string]OnMessageHandler{
				"unknown": makeOnMessageHandler("unknown", &log, false),
			},
		},
		{
			name:                 "no handler registered for queue",
			messageQueue:         "test",
			messageData:          `{"name":"test","meta":{"uuid":"12345","published_at":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:      []string{},
			errorsAfterUnmarshal: false,
			errorsAfterReceive:   true,

			onMessageHandlerQueue: "unknown",
			onMessageHandlers: map[string]OnMessageHandler{
				"test": makeOnMessageHandler("test", &log, false),
			},
		},
		{
			name:                 "an invalid message",
			messageQueue:         "test",
			messageData:          `{"name":"test","meta":{"published_at":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:      []string{},
			errorsAfterUnmarshal: true,
			errorsAfterReceive:   true,

			onMessageHandlerQueue: "test",
			onMessageHandlers: map[string]OnMessageHandler{
				"test": makeOnMessageHandler("test", &log, false),
			},
		},
		{
			name:                 "handler returns an error",
			messageQueue:         "test",
			messageData:          `{"name":"test","meta":{"uuid":"12345","published_at":"2021-10-10T12:32:00Z"},"payload":""}`,
			logAfterReceive:      []string{"test"},
			errorsAfterUnmarshal: false,
			errorsAfterReceive:   true,

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

			delivery, err := newTestDelivery([]byte(tc.messageData), 1)
			if tc.errorsAfterUnmarshal {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}

			result := receiver.Receive(context.Background(), tc.messageQueue, delivery)
			if tc.errorsAfterReceive {
				assert.Error(t, result.GetResult().Err)
			} else {
				assert.NoError(t, result.GetResult().Err)
			}

			assert.Len(t, log, len(tc.logAfterReceive))
			assert.Equal(t, tc.logAfterReceive, log)
		})
	}
}

func makeOnMessageHandler(name string, log *[]string, returnsErr bool) OnMessageHandler {
	return func(_ context.Context, _ string, _ Delivery) ResultContainer {
		*log = append(*log, name)

		if returnsErr {
			return FatalResult(errors.New("it failed"))
		}

		return SuccessResult()
	}
}

type testDelivery struct {
	attempt int
	message *Message
}

func newTestDelivery(data []byte, attempt int) (*testDelivery, error) {
	msg := &Message{}
	if err := json.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return &testDelivery{attempt: attempt, message: msg}, nil
}

func (d *testDelivery) GetAttempt() int {
	return d.attempt
}

func (d *testDelivery) GetMessage() *Message {
	return d.message
}
