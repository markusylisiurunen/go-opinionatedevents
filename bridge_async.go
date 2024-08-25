package opinionatedevents

import (
	"context"
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

func newAsyncBridge(maxDeliveryAttempts int, waitBetweenAttempts int, destinations ...Destination) *asyncBridge {
	return &asyncBridge{
		destinations: destinations,
		deliveryConfig: &asyncBridgeDeliveryConfig{
			maxAttempts: maxDeliveryAttempts,
			waitBetween: waitBetweenAttempts,
		},
	}
}

func (b *asyncBridge) take(ctx context.Context, batch []*Message) *envelope {
	env := newEnvelope(ctx, batch)
	go b.deliver(env)
	return env
}

func (b *asyncBridge) deliver(envelope *envelope) {
	destinations := b.destinations
	attemptsLeft := b.deliveryConfig.maxAttempts
	// attempt to deliver until no more attempts left
	for attemptsLeft > 0 {
		attemptsLeft -= 1
		// try delivering the message to all (pending) destinations
		deliveredTo := []int{}
		for i, destination := range destinations {
			if err := destination.Deliver(envelope.ctx, envelope.batch); err == nil {
				deliveredTo = append(deliveredTo, i)
			}
		}
		// remove the delivered destinations from the pending list
		tmp := []Destination{}
		for i, destination := range destinations {
			successful := false
			for _, u := range deliveredTo {
				if u == i {
					successful = true
					break
				}
			}
			if !successful {
				tmp = append(tmp, destination)
			}
		}
		destinations = tmp
		// if there are no more pending destinations, we are done
		if len(destinations) == 0 {
			envelope.closeWith(newDeliveryEvent(deliveryEventSuccessName))
			break
		}
		// otherwise, possibly try again or fail with an error
		if attemptsLeft > 0 {
			waitFor := time.Duration(b.deliveryConfig.waitBetween)
			time.Sleep(waitFor * time.Millisecond)
		} else {
			envelope.closeWith(newDeliveryEvent(deliveryEventFailureName))
		}
	}
}
