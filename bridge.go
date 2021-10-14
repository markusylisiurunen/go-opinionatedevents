package opinionatedevents

type bridge interface {
	take(m *Message) error
	drain()
}
