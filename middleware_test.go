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
	queue := "test"
	handler := WithLimit(3)(
		func(ctx context.Context, queue string, delivery Delivery) ResultContainer {
			return ErrorResult(errors.New("just a test"), time.Second)
		},
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	r1 := handler(context.Background(), queue, &testDelivery{1, msg})
	assert.NotNil(t, r1.GetResult().Err)
	assert.False(t, r1.GetResult().RetryAt.IsZero())
	r2 := handler(context.Background(), queue, &testDelivery{2, msg})
	assert.NotNil(t, r2.GetResult().Err)
	assert.False(t, r2.GetResult().RetryAt.IsZero())
	r3 := handler(context.Background(), queue, &testDelivery{3, msg})
	assert.NotNil(t, r3.GetResult().Err)
	assert.True(t, r3.GetResult().RetryAt.IsZero())
}

func TestBackoffMiddleware(t *testing.T) {
	queue := "test"
	handler := WithBackoff(LinearBackoff(2, 1, 10*time.Second))(
		func(ctx context.Context, queue string, delivery Delivery) ResultContainer {
			return ErrorResult(errors.New("just a test"), time.Second)
		},
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	r1 := handler(context.Background(), queue, &testDelivery{1, msg})
	assert.NotNil(t, r1.GetResult().Err)
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r1.GetResult().RetryAt).Milliseconds()-2000)), 10.0)
	r2 := handler(context.Background(), queue, &testDelivery{2, msg})
	assert.NotNil(t, r2.GetResult().Err)
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r2.GetResult().RetryAt).Milliseconds()-3000)), 10.0)
	r3 := handler(context.Background(), queue, &testDelivery{3, msg})
	assert.NotNil(t, r3.GetResult().Err)
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r3.GetResult().RetryAt).Milliseconds()-4000)), 10.0)
}

func TestLimitAndBackoffMiddlewares(t *testing.T) {
	queue := "test"
	handler := WithLimit(3)(
		WithBackoff(LinearBackoff(2, 1, 10*time.Second))(
			func(ctx context.Context, queue string, delivery Delivery) ResultContainer {
				return ErrorResult(errors.New("just a test"), time.Second)
			},
		),
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	r1 := handler(context.Background(), queue, &testDelivery{1, msg})
	assert.NotNil(t, r1.GetResult().Err)
	assert.False(t, r1.GetResult().RetryAt.IsZero())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r1.GetResult().RetryAt).Milliseconds()-2000)), 10.0)
	r2 := handler(context.Background(), queue, &testDelivery{2, msg})
	assert.NotNil(t, r2.GetResult().Err)
	assert.False(t, r2.GetResult().RetryAt.IsZero())
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r2.GetResult().RetryAt).Milliseconds()-3000)), 10.0)
	r3 := handler(context.Background(), queue, &testDelivery{3, msg})
	assert.NotNil(t, r3.GetResult().Err)
	assert.True(t, r3.GetResult().RetryAt.IsZero())
}
