package scfsio

import (
	"context"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
	"github.com/veritone/realtime/modules/scfs"
	"time"
)

type scfsReader struct {
	id        string
	scfsIO    scfs.IO
	chunkSize int64
}

func NewSCFSIOReader(id string, scfsIO scfs.IO) streamio.Streamer {
	return &scfsReader{
		id:     "",
		scfsIO: scfsIO,
	}
}

func (sc *scfsReader) Stream(ctx context.Context) *streamio.Stream {
	stream := streamio.NewStreamFromReader(sc.scfsIO.GetStreamReader(ctx, time.Duration(0), time.Duration(0)))
	return stream

}
