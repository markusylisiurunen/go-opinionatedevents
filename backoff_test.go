package opinionatedevents

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConstantBackoff(t *testing.T) {
	backoff := ConstantBackoff(5 * time.Second)
	expected, attempt := []int64{5, 5, 5}, 2
	for i := 0; i < len(expected); i += 1 {
		delay := backoff.DeliverAfter(attempt)
		assert.Equal(t, float64(expected[i]), delay.Seconds())
		attempt += 1
	}
}

func TestLinearBackoff(t *testing.T) {
	c, k := 5.0, 2.0
	backoff := LinearBackoff(c, k, 15*time.Second)
	expected, attempt := []int64{5, 7, 9, 11, 13, 15, 15}, 2
	for i := 0; i < len(expected); i += 1 {
		delay := backoff.DeliverAfter(attempt)
		assert.Equal(t, float64(expected[i]), delay.Seconds())
		attempt += 1
	}
}

func TestExponentialBackoff(t *testing.T) {
	c, a, b := 5.0, 2.0, 2.0
	backoff := ExponentialBackoff(c, a, b, 10000*time.Second)
	expected, attempt := []int64{5, 18, 112, 810, 5965, 10000}, 2
	for i := 0; i < len(expected); i += 1 {
		delay := backoff.DeliverAfter(attempt)
		assert.Equal(t, float64(expected[i]), delay.Seconds())
		attempt += 1
	}
}
