package opinionatedevents

type destination interface {
	deliver(msg *Message) error
}
