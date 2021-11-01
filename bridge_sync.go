package opinionatedevents

type syncBridge struct {
	destinations []Destination
}

func (b *syncBridge) take(msg *Message) *envelope {
	env := newEnvelope(msg)

	var possibleErr error = nil

	for _, d := range b.destinations {
		if err := d.Deliver(msg); err != nil {
			possibleErr = err
		}
	}

	if possibleErr != nil {
		env.closeWith(
			newDeliveryEvent(deliveryEventFailureName),
		)

		return env
	}

	env.closeWith(
		newDeliveryEvent(deliveryEventSuccessName),
	)

	return env
}

func newSyncBridge(destinations ...Destination) *syncBridge {
	return &syncBridge{
		destinations: destinations,
	}
}
