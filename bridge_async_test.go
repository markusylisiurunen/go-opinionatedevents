package opinionatedevents

import (
	"fmt"
	"testing"
	"time"
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

	t.Run("retries delivering a failed message only for failed destinations", func(t *testing.T) {
		destination1 := newTestDestination()
		destination2 := newTestDestination()

		bridge := newAsyncBridge(destination1, destination2)

		bridge.deliveryConfig.waitBetween = 0

		attempts := []int{0, 0}

		for i, d := range []*testDestination{destination1, destination2} {
			attemptIndex := i
			shouldFail := i == 0

			for u := 0; u < bridge.deliveryConfig.maxAttempts; u += 1 {
				d.pushHandler(func(_ *Message) error {
					attempts[attemptIndex] = attempts[attemptIndex] + 1

					if shouldFail {
						return fmt.Errorf("delivery failed")
					} else {
						return nil
					}
				})
			}
		}

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Error(err.Error())
			return
		}

		bridge.drain()

		if attempts[0] != 3 {
			t.Errorf("expected destination 1 call count to be 3 but was %d", attempts[0])
		}

		if attempts[1] != 1 {
			t.Errorf("expected destination 2 call count to be 1 but was %d", attempts[1])
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

	t.Run("drain waits for slow deliveries", func(t *testing.T) {
		destination := newTestDestination()
		bridge := newAsyncBridge(destination)

		waitFor := 500

		destination.pushHandler(func(_ *Message) error {
			time.Sleep(time.Duration(waitFor) * time.Millisecond)
			return nil
		})

		if err := bridge.take(NewMessage("test")); err != nil {
			t.Error(err.Error())
			return
		}

		start := time.Now()
		bridge.drain()

		duration := time.Since(start).Milliseconds()

		if duration < int64(waitFor) {
			t.Errorf("expected duration to be at least %d ms, it was %d ms", waitFor, duration)
		}
	})
}
