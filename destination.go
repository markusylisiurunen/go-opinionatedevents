package opinionatedevents

type destination interface {
	deliver(m *Message) error
}
