package opinionatedevents

import "context"

type Destination interface {
	Deliver(ctx context.Context, msg *Message) error
}
