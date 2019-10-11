package streamio

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

type fileReader struct {
	file         *os.File
	mimeType     string
	ffmpegFormat string
}

// NewFileReader initializes and returns a Streamer instance that reads a stream from a file
func NewFileReader(file *os.File, mimeType, ffmpegFormat string) Streamer {
	return &fileReader{
		file:         file,
		mimeType:     mimeType,
		ffmpegFormat: ffmpegFormat,
	}
}

func (fr *fileReader) Stream(ctx context.Context) *Stream {
	stream := NewStreamFromReader(fr.file)
	defer stream.done()
	stream.StartTime = time.Now()
	stream.MimeType = fr.mimeType
	stream.SetFfmpegFormat(fr.ffmpegFormat)
	stream.close = func() error {
		return fr.file.Close()
	}

	seekable := true
	if _, err := fr.file.Seek(0, 0); err != nil {
		seekable = false
	}

	if stream.FfmpegFormat.Name == "" && stream.MimeType == "" {
		// peek head of input stream to determine mime type
		if err := stream.GuessMimeType(); err != nil {
			stream.SendErr(fmt.Errorf("failed to determine mime type: %s", err))
			return stream
		}
	}

	// QT/MP4 files need to be processed in two passes, create a temp file for second pass
	if stream.IsQTFormat() {
		if !seekable {
			tempFile, err := os.Create(path.Join(os.TempDir(), "temp.mp4"))
			if err != nil {
				stream.SendErr(fmt.Errorf("failed to create temp file for seeking: %s", err))
				return stream
			}

			// stream.Tee(tempFile)

			// on close, re-assign buffer to cloned file
			stream.close = func() error {
				err := fr.file.Close()
				fr.file = tempFile
				return err
			}
		} else {
			stream.close = nil
		}
	}

	return stream
}

type fileWriter struct {
	file         *os.File
	bytesWritten *Progress
}

// NewMessageStreamWriter initializes and returns a new StreamWriter instance
func NewFileStreamWriter(file *os.File) StreamWriter {
	return &fileWriter{
		file:         file,
		bytesWritten: new(Progress),
	}
}

func (s *fileWriter) BytesWritten() int64 {
	return s.bytesWritten.Count()
}

func (s *fileWriter) WriteStream(ctx context.Context, stream *Stream) error {
	_, err := io.Copy(io.MultiWriter(s.file, s.bytesWritten), stream)
	return err
}
