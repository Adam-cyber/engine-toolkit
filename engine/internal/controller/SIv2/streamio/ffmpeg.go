package streamio

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/armon/circbuf"
)

const (
	containerMimeType = ""
	containerFormat   = "nut"
)

type ffmpegReader struct {
	url string
}

// NewFFMPEGReader initializes and returns a Streamer instance that reads an ffmpeg stream
func NewFFMPEGReader(url string) Streamer {
	return &ffmpegReader{url: url}
}

func (f *ffmpegReader) Stream(ctx context.Context) *Stream {
	// FFMPEG arguments
	args := []string{
		"-i", f.url,
		"-c", "copy",
		"-f", containerFormat,
	}

	stream := NewStream()
	stream.MimeType = containerMimeType
	stream.SetFfmpegFormat(containerFormat)
	stream.StartTime = time.Now()

	go func() {
		defer stream.done()
		err := runFFMPEG(ctx, append(args, "-"), stream)
		stream.SendErr(err)
	}()

	return stream
}

func runFFMPEG(ctx context.Context, args []string, stdout io.WriteCloser) error {
	defer stdout.Close()

	Logger.Printf("FFMPEG %v", args)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = stdout

	// write stderr stream to stdout and a buffer so errors can be parsed out
	stderrBuf, err := circbuf.NewBuffer(1024)
	if err != nil {
		return err
	}

	// cmd.Stderr = io.MultiWriter(os.Stderr, stderrBuf) // TODO configurable
	cmd.Stderr = stderrBuf

	if err := cmd.Run(); err != nil {
		Logger.Println("-------------------------------------------")
		Logger.Println("ffmpeg [reader] err:", err)
		Logger.Println("-------------------------------------------")
		Logger.Println(string(stderrBuf.Bytes()))
		Logger.Println("-------------------------------------------")
		err = parseFFMEGError(stderrBuf.Bytes(), err)
		return fmt.Errorf("ffmpeg [reader] command failed: %s", err)
	}

	return nil
}

func parseFFMEGError(stderr []byte, err error) error {
	errText := err.Error()

	if errText != "signal: killed" {
		// scan to last line of stderr buffer and use for error message
		br := bytes.NewReader(stderr)
		scanner := bufio.NewScanner(br)
		for scanner.Scan() {
			errText = scanner.Text()
		}
	}

	return errors.New(errText)
}
