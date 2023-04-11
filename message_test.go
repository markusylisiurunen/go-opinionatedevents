package opinionatedevents

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessageSerialization(t *testing.T) {
	t.Run("marshals and unmarshals correctly", func(t *testing.T) {
		// init the message
		message, err := NewMessage("test.test", &testMessagePayload{Value: "42"})
		assert.NoError(t, err)
		// marshal and unmarshal it
		serialized, err := message.MarshalJSON()
		assert.NoError(t, err)
		unserialized := &Message{}
		err = json.Unmarshal(serialized, unserialized)
		assert.NoError(t, err)
		// assert that they are equal
		assert.Equal(t, message.uuid, unserialized.uuid)
		assert.Equal(t, message.name, unserialized.name)
		assert.Equal(t, message.publishedAt.UTC().Format(time.RFC3339), unserialized.publishedAt.UTC().Format(time.RFC3339))
		assert.Equal(t, message.payload, unserialized.payload)
		// assert the payload
		payload := &testMessagePayload{}
		err = unserialized.GetPayload(payload)
		assert.NoError(t, err)
		assert.Equal(t, "42", payload.Value)
	})

	t.Run("does not accept invalid JSON", func(t *testing.T) {
		messages := []struct {
			value string
			valid bool
		}{
			// valid
			{value: `{"name":"test","meta":{"uuid":"12345","published_at":"2021-10-10T12:32:00Z"},"payload":""}`, valid: true},
			// missing name
			{value: `{"meta":{"uuid":"12345","published_at":"2021-10-10T12:32:00Z"},"payload":""}`, valid: false},
			// missing uuid
			{value: `{"name":"test","meta":{"published_at":"2021-10-10T12:32:00Z"},"payload":""}`, valid: false},
			// missing published at
			{value: `{"name":"test","meta":{"uuid":"12345"},"payload":""}`, valid: false},
		}
		for i, message := range messages {
			unserialized := &Message{}
			err := json.Unmarshal([]byte(message.value), unserialized)
			if message.valid {
				assert.NoError(t, err, fmt.Sprintf("error at index %d\n", i))
			} else {
				assert.Error(t, err, fmt.Sprintf("error at index %d\n", i))
			}
		}
	})
}

type testMessagePayload struct {
	Value string `json:"value"`
}
