package opinionatedevents

import "context"

type bridge interface {
	take(ctx context.Context, msg *Message) *envelope
}
