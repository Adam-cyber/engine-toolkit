// ADDED BY DROP - https://github.com/matryer/drop (v0.7)
//  source: github.com/dahernan/backoff /. (16994679f40f16943ebe2f96cb96aa9c89913f1a)
//  update: drop -f github.com/dahernan/backoff .
// license: Apache License (see repo for details)

package backoff

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	okAfter := 5
	calls := 0

	b := NewDoubleTimeBackoff(100*time.Millisecond, 1*time.Second, 10)
	err := b.Do(context.Background(), func() error {
		calls++
		if calls > okAfter {
			return nil
		}
		return errors.New("not ok")
	})
	if err != nil {
		t.Fatalf("Error should be nil but is: %v", err)
	}
	if calls > 10 {
		t.Fatalf("Calls should be < 10 but is: %v", calls)
	}
}

func TestBackoffMaxCalls(t *testing.T) {
	okAfter := 5
	calls := 0

	b := NewDoubleTimeBackoff(100*time.Millisecond, 1*time.Second, 2)
	err := b.Do(context.Background(), func() error {
		calls++
		if calls > okAfter {
			return nil
		}
		return errors.New("not ok")
	})
	if err == nil {
		t.Fatalf("Error should be not nil but is: %v", err)
	}
	if calls != 2 {
		t.Fatalf("Calls should be 2 but is: %v", calls)
	}
}

func TestBackoffErrAbort(t *testing.T) {
	calls := 0
	b := NewDoubleTimeBackoff(100*time.Millisecond, 1*time.Second, 2)
	err := b.Do(context.Background(), func() error {
		calls++
		return ErrAbort
	})
	if calls != 1 {
		t.Errorf("calls should be 1 but was %d", calls)
	}
	if err != ErrAbort {
		t.Errorf("ErrAbort should be returned")
	}
}
