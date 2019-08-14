package main

import (
	"log"
	"time"
)

type retrier interface {
	// Do executes the function doing retries in case it returns an error
	Do(f func() error) error
}

type doubleTimeBackoff struct {
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxCalls       int
}

// NewDoubleTimeBackoff retries the function f backing off the double of
// time each retry until a successfully call is made.
// initialBackoff is minimal time to wait for the next call
// maxBackoff is the maximum time between calls, if is 0 there is no maximum
// maxCalls is the maximum number of calls to the function, if is 0 there is no maximum
func newDoubleTimeBackoff(initialBackoff, maxBackoff time.Duration, maxCalls int) retrier {
	if initialBackoff == 0 {
		initialBackoff = 100 * time.Millisecond
	}
	return &doubleTimeBackoff{
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		maxCalls:       maxCalls,
	}
}

func (b *doubleTimeBackoff) Do(f func() error) error {
	backoff := time.Duration(0)
	calls := 0
	for {
		err := f()
		if err == nil {
			return nil
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
		log.Printf("[backoff %v] Retry after %v due to the Error: %v\n", calls, backoff, err)
		time.Sleep(backoff)
	}
}
