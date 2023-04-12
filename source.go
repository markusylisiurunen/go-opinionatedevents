package opinionatedevents

import "context"

type Source interface {
	Start(ctx context.Context, receiver *Receiver) error
}
