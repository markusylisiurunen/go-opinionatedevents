package opinionatedevents

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncBridge(t *testing.T) {
	ctx := context.Background()

	t.Run("fails if handler is not pushed", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)

		envelope := bridge.take(ctx, msg)
		assert.Error(t, waitForSuccessEnvelope(envelope))

		destination.pushHandler(func(_ context.Context, _ *Message) error {
			return nil
		})

		msg, err = NewMessage("test.test", nil)
		assert.NoError(t, err)

		envelope = bridge.take(ctx, msg)
		assert.NoError(t, waitForSuccessEnvelope(envelope))
	})

	t.Run("synchronously handles events", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		countToHandle := 5

		// keep track of how many messages have been handled
		handled := 0

		for i := 0; i < countToHandle; i++ {
			destination.pushHandler(func(_ context.Context, _ *Message) error {
				handled += 1
				return nil
			})

			// try to deliver the next message
			msg, err := NewMessage("test.test", nil)
			assert.NoError(t, err)

			envelope := bridge.take(ctx, msg)
			assert.NoError(t, waitForSuccessEnvelope(envelope))

			expected := i + 1

			if handled != expected {
				t.Errorf("total handled (%d) did not match (%d)", handled, expected)
			}
		}

		if handled != countToHandle {
			t.Errorf("total handled (%d) was not %d", handled, countToHandle)
		}
	})

	t.Run("synchronously waits for slow delivery", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		waitFor := 250

		destination.pushHandler(func(_ context.Context, _ *Message) error {
			time.Sleep(time.Duration(waitFor) * time.Millisecond)
			return nil
		})

		startAt := time.Now()

		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)

		envelope := bridge.take(ctx, msg)
		assert.NoError(t, waitForSuccessEnvelope(envelope))

		overAt := time.Now()

		if overAt.Sub(startAt).Milliseconds() < int64(waitFor) {
			t.Errorf("bridge returned too fast")
		}
	})

	t.Run("fails if message could not be delivered", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		destination.pushHandler(func(_ context.Context, _ *Message) error {
			return errors.New("failed")
		})

		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)

		envelope := bridge.take(ctx, msg)
		assert.Error(t, waitForSuccessEnvelope(envelope))
	})

	t.Run("returned delivery envelope resolves with success", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		destination.pushHandler(func(_ context.Context, _ *Message) error {
			return nil
		})

		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)

		envelope := bridge.take(ctx, msg)

		var success bool

		select {
		case <-envelope.onSuccess():
			success = true
		case <-envelope.onFailure():
			success = false
		}

		assert.True(t, success)
	})

	t.Run("returned delivery envelope resolves with failure", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		destination.pushHandler(func(_ context.Context, _ *Message) error {
			return errors.New("failed to deliver")
		})

		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)

		envelope := bridge.take(ctx, msg)

		var success bool

		select {
		case <-envelope.onSuccess():
			success = true
		case <-envelope.onFailure():
			success = false
		}

		assert.False(t, success)
	})
}
