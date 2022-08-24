package opinionatedevents

import (
	"context"
)

type OnMessageMiddleware func(next OnMessageHandler) OnMessageHandler

func WithBackoff(backoff Backoff) OnMessageMiddleware {
	return func(next OnMessageHandler) OnMessageHandler {
		return func(ctx context.Context, msg *Message) Result {
			res := next(ctx, msg)
			if res.error() == nil {
				return res
			}
			if res.retryAt().IsZero() {
				return res
			}
			attempt := msg.delivery.attempt + 1
			return ErrorResult(res.error(), ResultWithRetryAfter(backoff.DeliverAfter(attempt)))
		}
	}
}

func WithLimit(limit int) OnMessageMiddleware {
	return func(next OnMessageHandler) OnMessageHandler {
		return func(ctx context.Context, msg *Message) Result {
			res := next(ctx, msg)
			if res.error() == nil {
				return res
			}
			if msg.delivery.attempt >= limit {
				return ErrorResult(res.error(), ResultWithNoRetries())
			}
			return res
		}
	}
}
