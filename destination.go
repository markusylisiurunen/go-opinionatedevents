package opinionatedevents

type Destination interface {
	Deliver(msg *Message) error
}
