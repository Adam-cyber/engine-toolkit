package streamio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"

	qtfaststart "github.com/veritone/edge-stream-ingestor/qt-faststart"
)

const ReencodeTimeout = time.Minute * 90

var runCommandFn = runCommand

// Streamer is the interface that wraps the Stream method
type Streamer interface {
	Stream(ctx context.Context) *Stream
}

// FFProbeOutput is the output response of an ffprobe command
type FFProbeOutput struct {
	Streams []StreamInfo `json:"streams"`
	Format  Container    `json:"format"`
}

// StartStreamReader obtains a readable Stream from the provided Streamer instance
func StartStreamReader(ctx context.Context, sr Streamer) (*Stream, error) {
	stream := sr.Stream(ctx)
	if err := stream.Err(); err != nil {
		return stream, err
	}

	go func() {
		startTime := time.Now()
		logProgress := func() {
			Logger.Println("-- read: " + stream.bytesRead.Log(startTime))
		}
		defer logProgress()

		tick := time.NewTicker(time.Second)
		defer tick.Stop()

		for {
			select {
			case <-stream.donec:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				logProgress()
			}
		}
	}()

	// what is my stream?
	Logger.Println("StartStreamReader, [1] stream=", stream)
	if stream.MimeType == "" && stream.FfmpegFormat.Name == "" {
		err := stream.GuessMimeType()
		if err == ErrUnknownMimeType {
			Logger.Println(err)
		} else if err != nil {
			return stream, err
		}
	}
	Logger.Println("StartStreamReader, [2] stream=", stream)

	if stream.IsText() {
		return stream, nil
	}

	// Quicktime/MP4 format media cannot be streamed because necessary header information is often found at the
	// end of the file. This information must be moved to the beginning of the stream, and thus requires a second pass.
	if stream.IsQTFormat() {
		Logger.Println("Detected a Quicktime/MP4 formatted stream. First pass to parse moov atom...")

		qtf, err := qtfaststart.Parse(stream)
		if err != nil {
			// need to check for the other scenario!
			// best way is to check with ffprobe for the format...
			stream.Close()
			stream = sr.Stream(ctx)
			if res, err := checkWithFFProbe(ctx, stream); err == nil {
				if res, err = reencodeMP4(ctx, stream); err == nil {
					return res, err
				}
			}
			return stream, fmt.Errorf("failed to parse QT atoms from stream: %s", err)
		}

		// restart stream
		stream.Close()
		stream = sr.Stream(ctx)
		if err := stream.Err(); err != nil {
			return stream, err
		}

		if stream.MimeType == "" && stream.FfmpegFormat.Name == "" {
			if err := stream.GuessMimeType(); err != nil {
				return stream, err
			}
		}

		if !qtf.FastStartEnabled() {
			Logger.Println("Faststart not enabled. Making second pass to rearrange atoms...")
			r, err := qtfaststart.NewReader(stream.buf, qtf, false)
			if err != nil {
				return stream, fmt.Errorf("qt faststart conversion failed: %s", err)
			}

			stream.readFrom(r)
		} else {
			Logger.Println("Faststart already enabled. Re-reading stream...")
		}
	}

	stream, err := checkWithFFProbe(ctx, stream)
	if err == nil {
		stream, err = reencodeMP4(ctx, stream)
	}

	return stream, err
}

// Some mp4s have inconsistent DTS. Use ffmpeg to re-encode the stream before futher processing
func reencodeMP4(ctx context.Context, stream *Stream) (*Stream, error) {

	if stream.IsMP4Format() && !isMp4ButAACOnly(stream) {

		Logger.Println("re-encode MP4")

		args := append(stream.FfmpegFormat.Flags(),
			"-f", "mp4",
			"-i", "pipe:0",
			"-c", "copy",
			"-movflags", "frag_keyframe+empty_moov",
			"-f", "mp4",
			"-")

		ctxWithTimeout, cancel := context.WithTimeout(ctx, ReencodeTimeout)
		defer cancel()

		Logger.Printf("Executing ffmpeg %v", args)
		cmd := exec.CommandContext(ctxWithTimeout, "ffmpeg", args...)

		stdoutBuf, stderrBuf := new(bytes.Buffer), new(bytes.Buffer)
		cmd.Stdin = stream
		cmd.Stdout = stdoutBuf
		cmd.Stderr = stderrBuf

		if err := runCommandFn(cmd); err != nil {
			Logger.Println("-------------------------------------------")
			Logger.Println("ffmpeg err:", err)
			Logger.Println("------------------output-------------------")
			Logger.Println(string(stderrBuf.Bytes()))
			Logger.Println("------------------output-------------------")

			return stream, parseFFMEGError(stderrBuf.Bytes(), err)
		}

		stream.readFrom(stdoutBuf) // re-read bytes read out by ffmpeg

	}

	return stream, nil
}

func isMp4ButAACOnly(stream *Stream) bool {
	for _, info := range stream.Streams {
		if info.CodecType != "audio" {
			return false
		}
	}
	Logger.Println("is Mp4 but only has AAC stream")
	return true
}

func checkWithFFProbe(ctx context.Context, stream *Stream) (*Stream, error) {
	probeInfo, err := ProbeStream(ctx, stream)
	if err != nil {
		return stream, fmt.Errorf("ffprobe failed: %s", err)
	}

	stream.Streams = probeInfo.Streams
	stream.Container = probeInfo.Format
	if stream.FfmpegFormat.Name == "" {
		stream.FfmpegFormat.Name = stream.Container.FormatName
		head, err := stream.buf.Peek(bufferPeekSize)
		if err != nil {
			return stream, fmt.Errorf("ffprobe failed while peeking buffered input stream: %s", err)
		}
		contentType := http.DetectContentType(head)
		stream.MimeType = contentType
	}

	// normalize MIME type
	switch stream.MimeType {
	case "audio/mp3":
		stream.MimeType = "audio/mpeg"
	case "audio/x-wav", "audio/x-wave", "audio/wave":
		stream.MimeType = "audio/wav"
	}

	return stream, nil
}

// ProbeStream probes the given stream by peeking at bufferPeekSize bytes and returns the resulting
// ffprobe output.
func ProbeStream(ctx context.Context, stream *Stream) (FFProbeOutput, error) {
	var output FFProbeOutput

	head, err := stream.buf.Peek(bufferPeekSize)
	if err != nil && err != io.EOF {
		return output, fmt.Errorf("error while peeking buffered input stream: %s", err)
	}

	if len(head) == 0 {
		return output, errors.New("empty buffer read")
	}

	args := append(stream.FfmpegFormat.Flags(),
		"-i", "pipe:0",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		"-hide_banner",
	)

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	Logger.Printf("Executing ffprobe %v", args)
	cmd := exec.CommandContext(ctxWithTimeout, "ffprobe", args...)

	stdinBuf, stdoutBuf, stderrBuf := new(bytes.Buffer), new(bytes.Buffer), new(bytes.Buffer)
	cmd.Stdin = io.TeeReader(stream, stdinBuf) // copy bytes read by ffprobe into a buffer
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	if err := runCommandFn(cmd); err != nil {
		Logger.Println("-------------------------------------------")
		Logger.Println("ffprobe err:", err)
		Logger.Println("------------------output-------------------")
		Logger.Println(string(stderrBuf.Bytes()))
		Logger.Println("------------------output-------------------")

		return output, parseFFMEGError(stderrBuf.Bytes(), err)
	}

	stream.readFrom(io.MultiReader(stdinBuf, stream.buf)) // re-read bytes read out by ffprobe
	return output, json.Unmarshal(stdoutBuf.Bytes(), &output)
}

func runCommand(cmd *exec.Cmd) error {
	return cmd.Run()
}
