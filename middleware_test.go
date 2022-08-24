package opinionatedevents

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimitMiddleware(t *testing.T) {
	handler := WithLimit(3)(
		func(ctx context.Context, msg *Message) Result {
			return ErrorResult(errors.New("just a test"), ResultWithRetryAfter(time.Second))
		},
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	msg.delivery = messageDeliveryMeta{attempt: 1}
	r1 := handler(context.Background(), msg)
	assert.NotNil(t, r1.error())
	assert.False(t, r1.retryAt().IsZero())
	msg.delivery = messageDeliveryMeta{attempt: msg.delivery.attempt + 1}
	r2 := handler(context.Background(), msg)
	assert.NotNil(t, r2.error())
	assert.False(t, r2.retryAt().IsZero())
	msg.delivery = messageDeliveryMeta{attempt: msg.delivery.attempt + 1}
	r3 := handler(context.Background(), msg)
	assert.NotNil(t, r3.error())
	assert.True(t, r3.retryAt().IsZero())
}

func TestBackoffMiddleware(t *testing.T) {
	handler := WithBackoff(LinearBackoff(2, 1, 10*time.Second))(
		func(ctx context.Context, msg *Message) Result {
			return ErrorResult(errors.New("just a test"), ResultWithRetryAfter(time.Second))
		},
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	msg.delivery = messageDeliveryMeta{attempt: 1}
	r1 := handler(context.Background(), msg)
	assert.NotNil(t, r1.error())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r1.retryAt()).Milliseconds()-2000)), 10.0)
	msg.delivery = messageDeliveryMeta{attempt: msg.delivery.attempt + 1}
	r2 := handler(context.Background(), msg)
	assert.NotNil(t, r2.error())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r2.retryAt()).Milliseconds()-3000)), 10.0)
	msg.delivery = messageDeliveryMeta{attempt: msg.delivery.attempt + 1}
	r3 := handler(context.Background(), msg)
	assert.NotNil(t, r3.error())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r3.retryAt()).Milliseconds()-4000)), 10.0)
}

func TestLimitAndBackoffMiddlewares(t *testing.T) {
	handler := WithLimit(3)(
		WithBackoff(LinearBackoff(2, 1, 10*time.Second))(
			func(ctx context.Context, msg *Message) Result {
				return ErrorResult(errors.New("just a test"), ResultWithRetryAfter(time.Second))
			},
		),
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	msg.delivery = messageDeliveryMeta{attempt: 1}
	r1 := handler(context.Background(), msg)
	assert.NotNil(t, r1.error())
	assert.False(t, r1.retryAt().IsZero())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r1.retryAt()).Milliseconds()-2000)), 10.0)
	msg.delivery = messageDeliveryMeta{attempt: msg.delivery.attempt + 1}
	r2 := handler(context.Background(), msg)
	assert.NotNil(t, r2.error())
	assert.False(t, r2.retryAt().IsZero())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r2.retryAt()).Milliseconds()-3000)), 10.0)
	msg.delivery = messageDeliveryMeta{attempt: msg.delivery.attempt + 1}
	r3 := handler(context.Background(), msg)
	assert.NotNil(t, r3.error())
	assert.True(t, r3.retryAt().IsZero())
}
