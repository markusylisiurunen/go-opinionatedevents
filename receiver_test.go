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
			// reset the log
			log = []string{}
			// init the receiver and the handlers
			receiver, err := NewReceiver()
			assert.NoError(t, err)
			for name, onMessageHandler := range tc.onMessageHandlers {
				assert.NoError(t, receiver.On(name, tc.onMessageHandlerQueue, onMessageHandler))
			}
			assert.NoError(t, receiver.Start(context.Background()))
			// attempt to create a delivery
			delivery, err := newTestDelivery(tc.messageQueue, 1, []byte(tc.messageData))
			if tc.errorsAfterUnmarshal {
				assert.Error(t, err)
				return
			} else {
				assert.NoError(t, err)
			}
			// attempt to receive the delivery
			result := receiver.Deliver(context.Background(), delivery)
			if tc.errorsAfterReceive {
				assert.Error(t, result)
			} else {
				assert.NoError(t, result)
			}
			assert.Len(t, log, len(tc.logAfterReceive))
			assert.Equal(t, tc.logAfterReceive, log)
		})
	}
}

func makeOnMessageHandler(name string, log *[]string, returnsErr bool) OnMessageHandler {
	return func(_ context.Context, _ Delivery) error {
		*log = append(*log, name)
		if returnsErr {
			return Fatal(errors.New("it failed"))
		}
		return nil
	}
}

type testDelivery struct {
	attempt int
	queue   string
	message *Message
}

func newTestDelivery(queue string, attempt int, data []byte) (*testDelivery, error) {
	message := &Message{}
	if err := json.Unmarshal(data, message); err != nil {
		return nil, err
	}
	return &testDelivery{attempt: attempt, queue: queue, message: message}, nil
}

func (d *testDelivery) GetAttempt() int {
	return d.attempt
}

func (d *testDelivery) GetQueue() string {
	return d.queue
}

func (d *testDelivery) GetMessage() *Message {
	return d.message
}
