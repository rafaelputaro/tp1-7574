package internal

import (
	"math/rand"
	"time"
)

type StreamOpener[T any] func() (T, error)

// RetryWithBackoff tries to execute an operation with exponential backoff + jitter.
func RetryWithBackoff[T any](operation StreamOpener[T]) (T, error) {
	baseDelay := time.Second
	maxRetries := 5

	var stream T
	var err error

	for i := 0; i < maxRetries; i++ {
		stream, err = operation()
		if err == nil {
			return stream, nil
		}

		jitter := time.Duration(rand.Intn(500)) * time.Millisecond
		delay := baseDelay*(1<<i) + jitter

		logger.Infof("Retry %d: failed to execute operation: %v (retrying in %v)", i+1, err, delay)
		time.Sleep(delay)
	}

	return stream, err
}
