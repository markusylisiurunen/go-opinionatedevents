package opinionatedevents

import "context"

type bridge interface {
	take(ctx context.Context, batch []*Message) *envelope
}
