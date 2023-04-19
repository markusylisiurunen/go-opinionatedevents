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
		func(ctx context.Context, delivery Delivery) error {
			return errors.New("just a test")
		},
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	r1 := handler(context.Background(), &testDelivery{1, queue, msg})
	assert.NotNil(t, r1)
	assert.False(t, IsFatal(r1))
	r2 := handler(context.Background(), &testDelivery{2, queue, msg})
	assert.NotNil(t, r2)
	assert.False(t, IsFatal(r2))
	r3 := handler(context.Background(), &testDelivery{3, queue, msg})
	assert.NotNil(t, r3)
	assert.True(t, IsFatal(r3))
}

func TestBackoffMiddleware(t *testing.T) {
	queue := "test"
	handler := WithBackoff(LinearBackoff(2, 1, 10*time.Second))(
		func(ctx context.Context, delivery Delivery) error {
			return errors.New("just a test")
		},
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	r1 := handler(context.Background(), &testDelivery{1, queue, msg})
	assert.NotNil(t, r1)
	var r1r *retryError
	assert.True(t, errors.As(r1, &r1r))
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r1r.retryAt).Milliseconds()-2000)), 10.0)
	r2 := handler(context.Background(), &testDelivery{2, queue, msg})
	assert.NotNil(t, r2)
	var r2r *retryError
	assert.True(t, errors.As(r2, &r2r))
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r2r.retryAt).Milliseconds()-3000)), 10.0)
	r3 := handler(context.Background(), &testDelivery{3, queue, msg})
	assert.NotNil(t, r3)
	var r3r *retryError
	assert.True(t, errors.As(r3, &r3r))
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r3r.retryAt).Milliseconds()-4000)), 10.0)
}

func TestLimitAndBackoffMiddlewares(t *testing.T) {
	queue := "test"
	handler := WithLimit(3)(
		WithBackoff(LinearBackoff(2, 1, 10*time.Second))(
			func(ctx context.Context, delivery Delivery) error {
				return errors.New("just a test")
			},
		),
	)
	msg, err := NewMessage("test.test", &testMessagePayload{"test"})
	assert.NoError(t, err)
	r1 := handler(context.Background(), &testDelivery{1, queue, msg})
	assert.NotNil(t, r1)
	assert.False(t, IsFatal(r1))
	var r1r *retryError
	assert.True(t, errors.As(r1, &r1r))
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r1r.retryAt).Milliseconds()-2000)), 10.0)
	r2 := handler(context.Background(), &testDelivery{2, queue, msg})
	assert.NotNil(t, r2)
	assert.False(t, IsFatal(r2))
	var r2r *retryError
	assert.True(t, errors.As(r2, &r2r))
	assert.LessOrEqual(t, math.Abs(float64(time.Until(r2r.retryAt).Milliseconds()-3000)), 10.0)
	r3 := handler(context.Background(), &testDelivery{3, queue, msg})
	assert.NotNil(t, r3)
	assert.True(t, IsFatal(r3))
}
