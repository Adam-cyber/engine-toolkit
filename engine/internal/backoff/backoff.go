// ADDED BY DROP - https://github.com/matryer/drop (v0.7)
//  source: github.com/dahernan/backoff (16994679f40f16943ebe2f96cb96aa9c89913f1a)
//  update: drop -f github.com/dahernan/backoff
// license: Apache License (see repo for details)

package backoff

import (
	"context"
	"errors"
	"log"
	"time"
)

// ErrAbort indicates that the backoff should abort.
var ErrAbort = errors.New("abored")

// Retrier runs code many times.
type Retrier interface {
	// Do executes the function doing retries in case it returns an error
	Do(ctx context.Context, f func() error) error
}

// DoubleTimeBackoff provides backoff functionality.
type DoubleTimeBackoff struct {
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxCalls       int
}

// NewDoubleTimeBackoff retries the function f backing off the double of
// time each retry until a successfully call is made.
// initialBackoff is minimal time to wait for the next call
// maxBackoff is the maximum time between calls, if is 0 there is no maximum
// maxCalls is the maximum number of calls to the function, if is 0 there is no maximum
func NewDoubleTimeBackoff(initialBackoff, maxBackoff time.Duration, maxCalls int) Retrier {
	if initialBackoff == 0 {
		initialBackoff = 100 * time.Millisecond
	}
	return &DoubleTimeBackoff{
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		maxCalls:       maxCalls,
	}
}

// Do executes the function using the specified rules.
func (b *DoubleTimeBackoff) Do(ctx context.Context, f func() error) error {
	backoff := time.Duration(0)
	calls := 0
	for {
		err := f()
		if err == nil || err == ErrAbort {
			return err
		}
		calls++
		if (calls >= b.maxCalls) && (b.maxCalls != 0) {
			return err
		}
		switch {
		case backoff == 0:
			backoff = b.initialBackoff
		case (backoff >= b.maxBackoff) && (b.maxBackoff != 0):
			backoff = b.maxBackoff
		default:
			backoff *= 2
		}
		log.Printf("[backoff %d/%d] waiting %v: %v\n", calls, b.maxCalls, backoff, err)
		time.Sleep(backoff)
	}
}
