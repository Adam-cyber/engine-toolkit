package adapter

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/veritone/edge-stream-ingestor/streamio"
	"github.com/veritone/realtime/modules/scfs"
	"io"
	"log"
)

type scfsIOWriter struct {
	id           string
	scfsIO       scfs.IO
	bytesWritten *streamio.Progress
	chunkSize    int64
}

func NewSCFSIOWriter(id string, scfsIO scfs.IO, chunkSize int64) streamio.StreamWriter {
	return &scfsIOWriter{
		id:           id,
		scfsIO:       scfsIO,
		bytesWritten: new(streamio.Progress),
		chunkSize:    chunkSize,
	}
}

func (sw *scfsIOWriter) BytesWritten() int64 {
	return sw.bytesWritten.Count()
}

// what a bad name,
// this is really:  read from the stream and write to wherever your target destination is
func (sw *scfsIOWriter) WriteStream(ctx context.Context, stream *streamio.Stream) (err error) {
	tr := io.TeeReader(stream, sw.bytesWritten)

	writeChan := sw.scfsIO.GetStreamWriterAsChan(sw.chunkSize)
	defer close(writeChan)
	buffer := make([]byte, sw.chunkSize)

	for {
		n, err := tr.Read(buffer)
		if err != nil {
			if err == io.EOF {
				for i := 0; i < n; i++ {
					writeChan <- buffer[i] //should handle any remainding bytes.
				}
				break
			}
			return err
		}
		for i := 0; i < n; i++ {
			writeChan <- buffer[i] //should handle any remainding bytes.
		}
		// need to check for eror
		errArr := sw.scfsIO.GetStreamWriterAsChanErrors()
		if len(errArr) > 0 {
			// ding ding error
			return errors.Wrapf(err, "[scfsio:%s] WriteStream failed", sw.id)
		}

	}
	return
}

//============================================================================================

type TeeStreamWriter struct { // implements the io.Writer interface
	sw1 streamio.StreamWriter
	sw2 streamio.StreamWriter
}

func NewTeeStreamWriter(sw1 streamio.StreamWriter, sw2 streamio.StreamWriter) (streamio.StreamWriter, error) {

	return &TeeStreamWriter{
		sw1: sw1,
		sw2: sw2}, nil
}

func (tsw *TeeStreamWriter) WriteStream(ctx context.Context, stream *streamio.Stream) error {

	pr, pw := io.Pipe()

	stream.Tee(pw)

	clonedStream := streamio.NewStreamFromReader(pr)
	clonedStream.StartTime = stream.StartTime
	clonedStream.StartOffsetMS = stream.StartOffsetMS
	clonedStream.MimeType = stream.MimeType
	clonedStream.FfmpegFormat = stream.FfmpegFormat
	clonedStream.Streams = stream.Streams
	clonedStream.Container = stream.Container

	errc := make(chan error)

	// stream writer 1
	go func() {

		defer pw.Close()

		if err := tsw.sw1.WriteStream(ctx, stream); err != nil {
			log.Printf("stream writer 1 err: %s", err)
			errc <- fmt.Errorf("stream writer 1 err: %s", err)
		} else {
			errc <- nil
		}
	}()

	// stream writer 2
	go func() {
		if err := tsw.sw2.WriteStream(ctx, clonedStream); err != nil {
			log.Printf("stream writer 2 err: %s", err)
			errc <- fmt.Errorf("stream writer 2 err: %s", err)
		} else {
			errc <- nil
		}
	}()

	for i := 1; i <= 2; i++ { // geting responses from two channels
		errorInChan := <-errc
		if errorInChan != nil {
			fmt.Println(errorInChan)
			return errorInChan
		}
	}

	return nil

}

func (tsw *TeeStreamWriter) BytesWritten() int64 {
	return tsw.sw1.BytesWritten()
}
