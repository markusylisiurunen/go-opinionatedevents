package opinionatedevents

import (
	"context"
	"time"
)

type OnMessageMiddleware func(next OnMessageHandler) OnMessageHandler

func WithBackoff(backoff Backoff) OnMessageMiddleware {
	return func(next OnMessageHandler) OnMessageHandler {
		return func(ctx context.Context, delivery Delivery) error {
			err := next(ctx, delivery)
			// override the retry at time if an error was returned and it was not fatal
			if err != nil && !isFatal(err) {
				return &retryError{
					retryAt: time.Now().Add(backoff.DeliverAfter(delivery.GetAttempt() + 1)),
					err:     err,
				}
			}
			return err
		}
	}
}

func WithLimit(limit int) OnMessageMiddleware {
	return func(next OnMessageHandler) OnMessageHandler {
		return func(ctx context.Context, delivery Delivery) error {
			err := next(ctx, delivery)
			// override the error with a fatal error if all attempts have been used
			if err != nil && delivery.GetAttempt() >= limit {
				if isFatal(err) {
					return err
				}
				return Fatal(err)
			}
			return err
		}
	}
}
