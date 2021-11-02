package opinionatedevents

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessageSerialization(t *testing.T) {
	t.Run("marshals and unmarshals correctly", func(t *testing.T) {
		message := NewMessage("test")
		message.SetPayload(&testMessagePayload{Value: "42"})

		serialized, err := message.MarshalJSON()
		assert.NoError(t, err)

		unserialized, err := ParseMessage(serialized)
		assert.NoError(t, err)

		assert.Equal(t, message.name, unserialized.name)
		assert.Equal(t, message.meta.uuid, unserialized.meta.uuid)

		assert.Equal(t,
			message.meta.timestamp.Format(time.RFC3339),
			unserialized.meta.timestamp.Format(time.RFC3339),
		)

		assert.Equal(t, message.payload, unserialized.payload)

		payload := &testMessagePayload{}

		err = unserialized.Payload(payload)
		assert.NoError(t, err)

		assert.Equal(t, "42", payload.Value)
	})
}

type testMessagePayload struct {
	Value string `json:"value"`
}

func (p *testMessagePayload) MarshalPayload() ([]byte, error) {
	return json.Marshal(p)
}

func (p *testMessagePayload) UnmarshalPayload(data []byte) error {
	return json.Unmarshal(data, p)
}
