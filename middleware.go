package opinionatedevents

import (
	"context"
)

type OnMessageMiddleware func(next OnMessageHandler) OnMessageHandler

func WithBackoff(backoff Backoff) OnMessageMiddleware {
	return func(next OnMessageHandler) OnMessageHandler {
		return func(ctx context.Context, queue string, delivery Delivery) ResultContainer {
			res := next(ctx, queue, delivery)
			if res.GetResult().Err == nil || res.GetResult().RetryAt.IsZero() {
				return res
			}
			i := delivery.GetAttempt() + 1
			return ErrorResult(res.GetResult().Err, backoff.DeliverAfter(i))
		}
	}
}

func WithLimit(limit int) OnMessageMiddleware {
	return func(next OnMessageHandler) OnMessageHandler {
		return func(ctx context.Context, queue string, delivery Delivery) ResultContainer {
			res := next(ctx, queue, delivery)
			if res.GetResult().Err != nil && delivery.GetAttempt() >= limit {
				return FatalResult(res.GetResult().Err)
			}
			return res
		}
	}
}
