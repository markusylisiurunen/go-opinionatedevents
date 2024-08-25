package opinionatedevents

import "context"

type Destination interface {
	Deliver(ctx context.Context, batch []*Message) error
}
