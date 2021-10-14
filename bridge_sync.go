package opinionatedevents

type syncBridge struct {
	destinations []destination
}

func (b *syncBridge) take(m *Message) error {
	var possibleErr error = nil

	for _, d := range b.destinations {
		if err := d.deliver(m); err != nil {
			possibleErr = err
		}
	}

	if possibleErr != nil {
		return possibleErr
	}

	return nil
}

func (b *syncBridge) drain() {}

func newSyncBridge(destinations ...destination) *syncBridge {
	return &syncBridge{destinations: destinations}
}
