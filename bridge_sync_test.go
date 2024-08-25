package opinionatedevents

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSyncBridge(t *testing.T) {
	t.Run("fails if handler is not pushed", func(t *testing.T) {
		ctx := context.Background()
		destination := newTestDestination()
		bridge := newSyncBridge(destination)
		// construct the message
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		// pass it through the sync bridge
		envelope := bridge.take(ctx, []*Message{msg})
		assert.Error(t, waitForSuccessEnvelope(envelope))
		// try again with a handler defined
		destination.pushHandler(func(_ context.Context, _ []*Message) error {
			return nil
		})
		msg, err = NewMessage("test.test", nil)
		assert.NoError(t, err)
		envelope = bridge.take(ctx, []*Message{msg})
		assert.NoError(t, waitForSuccessEnvelope(envelope))
	})

	t.Run("synchronously handles events", func(t *testing.T) {
		ctx := context.Background()
		destination := newTestDestination()
		bridge := newSyncBridge(destination)
		// number of messages to handle
		countToHandle := 5
		// keep track of how many messages have been handled
		handled := 0
		for i := 0; i < countToHandle; i++ {
			// push the handler
			destination.pushHandler(func(_ context.Context, _ []*Message) error {
				handled += 1
				return nil
			})
			// deliver the next message
			msg, err := NewMessage("test.test", nil)
			assert.NoError(t, err)
			envelope := bridge.take(ctx, []*Message{msg})
			assert.NoError(t, waitForSuccessEnvelope(envelope))
			// check that it was handled
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
		ctx := context.Background()
		destination := newTestDestination()
		bridge := newSyncBridge(destination)
		// push the slow handler
		waitFor := 250
		destination.pushHandler(func(_ context.Context, _ []*Message) error {
			time.Sleep(time.Duration(waitFor) * time.Millisecond)
			return nil
		})
		// deliver the message
		startAt := time.Now()
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		envelope := bridge.take(ctx, []*Message{msg})
		assert.NoError(t, waitForSuccessEnvelope(envelope))
		overAt := time.Now()
		if overAt.Sub(startAt).Milliseconds() < int64(waitFor) {
			t.Errorf("bridge returned too fast")
		}
	})

	t.Run("fails if message could not be delivered", func(t *testing.T) {
		ctx := context.Background()
		destination := newTestDestination()
		bridge := newSyncBridge(destination)
		// push the failing handler
		destination.pushHandler(func(_ context.Context, _ []*Message) error {
			return errors.New("failed")
		})
		// deliver the message
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		envelope := bridge.take(ctx, []*Message{msg})
		assert.Error(t, waitForSuccessEnvelope(envelope))
	})

	t.Run("returned delivery envelope resolves with success", func(t *testing.T) {
		ctx := context.Background()
		destination := newTestDestination()
		bridge := newSyncBridge(destination)
		// push the handler
		destination.pushHandler(func(_ context.Context, _ []*Message) error {
			return nil
		})
		// deliver the message
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		envelope := bridge.take(ctx, []*Message{msg})
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
		ctx := context.Background()
		destination := newTestDestination()
		bridge := newSyncBridge(destination)
		// push the failing handler
		destination.pushHandler(func(_ context.Context, _ []*Message) error {
			return errors.New("failed to deliver")
		})
		// deliver the message
		msg, err := NewMessage("test.test", nil)
		assert.NoError(t, err)
		envelope := bridge.take(ctx, []*Message{msg})
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
