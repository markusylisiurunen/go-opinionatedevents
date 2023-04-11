package opinionatedevents

import "context"

type syncBridge struct {
	destinations []Destination
}

func newSyncBridge(destinations ...Destination) *syncBridge {
	return &syncBridge{destinations: destinations}
}

func (b *syncBridge) take(ctx context.Context, msg *Message) *envelope {
	env := newEnvelope(ctx, msg)
	var possibleErr error = nil
	for _, d := range b.destinations {
		if err := d.Deliver(ctx, msg); err != nil {
			possibleErr = err
		}
	}
	if possibleErr != nil {
		env.closeWith(newDeliveryEvent(deliveryEventFailureName))
		return env
	}
	env.closeWith(newDeliveryEvent(deliveryEventSuccessName))
	return env
}
