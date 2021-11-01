package opinionatedevents

import (
	"time"
)

type asyncBridgeDeliveryConfig struct {
	maxAttempts int
	waitBetween int
}

type asyncBridge struct {
	destinations   []Destination
	deliveryConfig *asyncBridgeDeliveryConfig
}

func (b *asyncBridge) take(msg *Message) *envelope {
	env := newEnvelope(msg)
	go b.deliver(env)
	return env
}

func (b *asyncBridge) deliver(envelope *envelope) {
	destinations := b.destinations
	attemptsLeft := b.deliveryConfig.maxAttempts

	for attemptsLeft > 0 {
		attemptsLeft -= 1

		// try delivering the message to all (pending) destinations
		deliveredDestinations := []int{}

		for i, destination := range destinations {
			if err := destination.Deliver(envelope.message); err == nil {
				deliveredDestinations = append(deliveredDestinations, i)
			}
		}

		// remove the delivered destinations from the pending list
		tmp := []Destination{}

		for i, destination := range destinations {
			// check if this destination was successful
			successful := false

			for _, u := range deliveredDestinations {
				if u == i {
					successful = true
					break
				}
			}

			// if it was not successful, we will try again
			if !successful {
				tmp = append(tmp, destination)
			}
		}

		destinations = tmp

		if len(destinations) == 0 {
			envelope.closeWith(
				newDeliveryEvent(deliveryEventSuccessName),
			)

			break
		}

		if attemptsLeft > 0 {
			waitFor := time.Duration(b.deliveryConfig.waitBetween)
			time.Sleep(waitFor * time.Millisecond)
		} else {
			envelope.closeWith(
				newDeliveryEvent(deliveryEventFailureName),
			)
		}
	}
}

func newAsyncBridge(
	maxDeliveryAttempts int,
	waitBetweenAttempts int,
	destinations ...Destination,
) *asyncBridge {
	bridge := &asyncBridge{
		destinations: destinations,

		deliveryConfig: &asyncBridgeDeliveryConfig{
			maxAttempts: maxDeliveryAttempts,
			waitBetween: waitBetweenAttempts,
		},
	}

	return bridge
}
