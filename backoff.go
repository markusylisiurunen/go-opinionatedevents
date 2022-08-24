package opinionatedevents

import (
	"math"
	"time"
)

type Backoff interface {
	DeliverAfter(attempt int) time.Duration
}

// constant backoff always applies the same duration regardless of the retry history
type constantBackoff struct {
	delay time.Duration
}

func ConstantBackoff(delay time.Duration) *constantBackoff {
	return &constantBackoff{delay}
}

func (b *constantBackoff) DeliverAfter(attempt int) time.Duration {
	return b.delay
}

// linear backoff applies the backoff according to: min(c + k * i, l)
type linearBackoff struct {
	c float64
	k float64
	l time.Duration
}

func LinearBackoff(c float64, k float64, l time.Duration) *linearBackoff {
	return &linearBackoff{c, k, l}
}

func (b *linearBackoff) DeliverAfter(attempt int) time.Duration {
	// NOTE: backoff will be first called for the 2nd attempt (i.e. attempt == 2)
	i := float64(attempt - 2)
	s := b.c + b.k*i
	if s >= b.l.Seconds() {
		return b.l
	}
	// the duration will be rounded to the closest second
	return time.Second * time.Duration(math.Round(s))
}

// exponential backoff applies the backoff according to: min(c + a * (e^(b * i) - 1), l)
type exponentialBackoff struct {
	c float64
	a float64
	b float64
	l time.Duration
}

func ExponentialBackoff(c float64, a float64, b float64, l time.Duration) *exponentialBackoff {
	return &exponentialBackoff{c, a, b, l}
}

func (b *exponentialBackoff) DeliverAfter(attempt int) time.Duration {
	// NOTE: backoff will be first called for the 2nd attempt (i.e. attempt == 2)
	i := float64(attempt - 2)
	s := b.c + b.a*(math.Pow(math.E, b.b*i)-1)
	if s > b.l.Seconds() {
		return b.l
	}
	// the duration will be rounded to the closest second
	return time.Second * time.Duration(math.Round(s))
}
