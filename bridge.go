package opinionatedevents

type bridge interface {
	take(msg *Message) error
	drain()
}
