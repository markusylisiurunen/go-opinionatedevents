package opinionatedevents

import (
	"fmt"
	"sync"
	"time"
)

type asyncBridgeDeliveryConfig struct {
	maxAttempts int
	waitBetween int
}

type asyncBridge struct {
	destinations   []destination
	wg             *sync.WaitGroup
	deliveryConfig *asyncBridgeDeliveryConfig
}

func (b *asyncBridge) take(msg *Message) error {
	b.wg.Add(1)
	go b.deliver(msg)
	return nil
}

func (b *asyncBridge) drain() {
	b.wg.Wait()
}

func (b *asyncBridge) deliver(msg *Message) {
	defer b.wg.Done()

	destinations := b.destinations
	attemptsLeft := b.deliveryConfig.maxAttempts

	for attemptsLeft > 0 {
		attemptsLeft -= 1

		// try delivering the message to all (pending) destinations
		deliveredDestinations := []int{}

		for i, destination := range destinations {
			if err := destination.deliver(msg); err != nil {
				// TODO: how to log errors?
				fmt.Printf("%s\n", err.Error())
				continue
			}

			deliveredDestinations = append(deliveredDestinations, i)
		}

		// remove the delivered destinations from the pending list
		tmp := []destination{}

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
			break
		}

		if attemptsLeft > 0 {
			waitFor := time.Duration(b.deliveryConfig.waitBetween)
			time.Sleep(waitFor * time.Millisecond)
		}
	}
}

func newAsyncBridge(
	maxDeliveryAttempts int,
	waitBetweenAttempts int,
	destinations ...destination,
) *asyncBridge {
	bridge := &asyncBridge{
		destinations: destinations,
		wg:           &sync.WaitGroup{},

		deliveryConfig: &asyncBridgeDeliveryConfig{
			maxAttempts: maxDeliveryAttempts,
			waitBetween: waitBetweenAttempts,
		},
	}

	return bridge
}
