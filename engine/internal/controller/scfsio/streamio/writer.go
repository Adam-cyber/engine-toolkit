package streamio

import (
	"context"
	"fmt"
	"time"
)

const defaultNoDataTimeout = time.Minute

// ErrWriterNoDataTimeout is returned when the stream writer has not written any data in a certain timeout interval
var ErrWriterNoDataTimeout = fmt.Errorf("timeout: stream writer has not written any new data in %s", defaultNoDataTimeout)

// StreamWriter is implemented by any type that supports writing a stream read from a Stream
// instance and returns the number of bytes written via the BytesWritten method.
type StreamWriter interface {
	WriteStream(context.Context, *Stream) error
	BytesWritten() int64
}
