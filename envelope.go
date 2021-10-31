package opinionatedevents

const (
	deliveryEventSuccessName string = "success"
	deliveryEventFailureName string = "failure"
)

type deliveryEvent struct {
	name string
}

func newDeliveryEvent(name string) *deliveryEvent {
	return &deliveryEvent{name: name}
}

type envelope struct {
	message *Message

	events  chan *deliveryEvent
	proxies []chan *deliveryEvent

	closedWith *deliveryEvent
}

func (e *envelope) onEvent(name string) chan struct{} {
	in := make(chan *deliveryEvent)
	out := make(chan struct{})

	e.proxies = append(e.proxies, in)

	go func() {
		for event := range in {
			if event.name == name {
				out <- struct{}{}
				break
			}
		}

		// TODO: should close the `out` channel
		// close(out)
	}()

	return out
}

func (e *envelope) onSuccess() chan struct{} {
	if e.closedWith != nil {
		out := make(chan struct{}, 1)

		if e.closedWith.name == deliveryEventSuccessName {
			out <- struct{}{}
		}

		return out
	}

	return e.onEvent(deliveryEventSuccessName)
}

func (e *envelope) onFailure() chan struct{} {
	if e.closedWith != nil {
		out := make(chan struct{}, 1)

		if e.closedWith.name == deliveryEventFailureName {
			out <- struct{}{}
		}

		return out
	}

	return e.onEvent(deliveryEventFailureName)
}

func (e *envelope) closeWith(event *deliveryEvent) {
	if e.closedWith != nil {
		return
	}

	e.closedWith = event

	go func() {
		e.events <- event
		close(e.events)
	}()
}

func newEnvelope(msg *Message) *envelope {
	env := &envelope{
		message: msg,

		events:  make(chan *deliveryEvent),
		proxies: []chan *deliveryEvent{},

		closedWith: nil,
	}

	go func() {
		for event := range env.events {
			for _, proxy := range env.proxies {
				proxy <- event
			}
		}

		for _, proxy := range env.proxies {
			close(proxy)
		}
	}()

	return env
}
