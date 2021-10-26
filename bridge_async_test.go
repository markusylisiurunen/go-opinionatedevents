package opinionatedevents

import (
	"fmt"
	"testing"
)

func TestAsyncBridge(t *testing.T) {
	t.Run("delivers a message", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newAsyncBridge(destination)

		bridge.deliveryConfig.waitBetween = 0

		attempts := 0

		destination.pushHandler(func(_ *Message) error {
			attempts += 1
			return nil
		})

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Error(err.Error())
			return
		}

		bridge.drain()

		if attempts != 1 {
			t.Errorf("expected destination call count to be 1 but was %d", attempts)
		}
	})

	t.Run("retries delivering a failed message", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newAsyncBridge(destination)

		bridge.deliveryConfig.waitBetween = 0

		attempts := 0

		for i := 0; i < 2; i++ {
			isFirstHandler := i == 0

			destination.pushHandler(func(_ *Message) error {
				attempts += 1

				if isFirstHandler {
					return fmt.Errorf("delivery failed")
				} else {
					return nil
				}
			})
		}

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Error(err.Error())
			return
		}

		bridge.drain()

		if attempts != 2 {
			t.Errorf("expected destination call count to be 2 but was %d", attempts)
		}
	})

	t.Run("gives up delivering a message after max attempts", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newAsyncBridge(destination)

		bridge.deliveryConfig.maxAttempts = 3
		bridge.deliveryConfig.waitBetween = 0

		attempts := 0

		for i := 0; i < bridge.deliveryConfig.maxAttempts+1; i++ {
			destination.pushHandler(func(_ *Message) error {
				attempts += 1
				return fmt.Errorf("delivery failed")
			})
		}

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Error(err.Error())
			return
		}

		bridge.drain()

		if attempts != bridge.deliveryConfig.maxAttempts {
			t.Errorf("expected destination call count to be %d but was %d",
				bridge.deliveryConfig.maxAttempts,
				attempts,
			)
		}
	})
}
