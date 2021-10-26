package opinionatedevents

import (
	"fmt"
	"testing"
	"time"
)

func TestSyncBridge(t *testing.T) {
	t.Run("fails if handler is not pushed", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		if err := bridge.take(NewMessage("test")); err == nil {
			t.Errorf("expected error to not be nil")
		}

		destination.pushHandler(func(_ *Message) error {
			return nil
		})

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Errorf("expected error to be nil")
		}
	})

	t.Run("synchronously handles events", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		countToHandle := 5

		// keep track of how many messages have been handled
		handled := 0

		for i := 0; i < countToHandle; i++ {
			destination.pushHandler(func(_ *Message) error {
				handled += 1
				return nil
			})

			// try to deliver the next message
			if err := bridge.take(NewMessage("test")); err != nil {
				t.Fatal(err)
			}

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

		destination.pushHandler(func(_ *Message) error {
			time.Sleep(time.Duration(waitFor) * time.Millisecond)
			return nil
		})

		startAt := time.Now()

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Fatal(err)
		}

		overAt := time.Now()

		if overAt.Sub(startAt).Milliseconds() < int64(waitFor) {
			t.Errorf("bridge returned too fast")
		}
	})

	t.Run("fails if message could not be delivered", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newSyncBridge(destination)

		expectedErr := fmt.Errorf("something went wrong")

		destination.pushHandler(func(_ *Message) error {
			return expectedErr
		})

		if err := bridge.take(NewMessage("test")); err != expectedErr {
			t.Errorf("expected error to not be nil")
		}
	})
}
