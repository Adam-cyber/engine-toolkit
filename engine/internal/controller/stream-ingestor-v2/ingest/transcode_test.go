package ingest

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"

	"github.com/stretchr/testify/suite"
	"github.com/veritone/edge-stream-ingestor/streamio"
)

type transcodeTestSuite struct {
	suite.Suite
}

func (t *transcodeTestSuite) TearDownTest() {
	runFFMPEGCommandFn = runFFMPEGCommand
}

func (t *transcodeTestSuite) TestTranscodeStream() {
	streamIn := streamio.NewStream()
	streamIn.FfmpegFormat.Name = "webm"
	streamIn.MimeType = "video/webm"

	runFFMPEGCommandFn = func(cmd *exec.Cmd, stdErrOut io.Writer) error {
		t.Equal([]string{"ffmpeg", "-f", "webm", "-i", "pipe:0", "-f", "s16le", "-acodec", "pcm_s16le", "-ac", "1", "-ar", "16000", "-"}, cmd.Args)
		t.Equal(streamIn, cmd.Stdin)
		if t.NotNil(cmd.Stdout) {
			_, err := cmd.Stdout.Write([]byte("an output stream"))
			t.NoError(err)
		}
		return nil
	}

	transcodeFormat := "s16le;acodec=pcm_s16le;ac=1;ar=16000"
	streamOut, errc := TranscodeStream(context.Background(), streamIn, transcodeFormat)
	streamOutBytes, _ := ioutil.ReadAll(streamOut)
	t.NoError(<-errc)
	t.Equal([]byte("an output stream"), streamOutBytes)
	t.Equal("s16le;acodec=pcm_s16le;ac=1;ar=16000", streamOut.FfmpegFormat.String())
	t.Equal("", streamOut.MimeType)

	// mjpeg without framerate (default to 1)
	transcodeFormat = "mjpeg"
	runFFMPEGCommandFn = func(cmd *exec.Cmd, stdErrOut io.Writer) error {
		t.Equal([]string{"ffmpeg", "-f", "webm", "-i", "pipe:0", "-f", "mjpeg", "-an", "-q:v", "3", "-vf", "fps=1.000E+00", "-"}, cmd.Args)
		return nil
	}

	streamOut, errc = TranscodeStream(context.Background(), streamIn, transcodeFormat)
	ioutil.ReadAll(streamOut)
	t.NoError(<-errc)
	t.Equal("mjpeg;framerate=1", streamOut.FfmpegFormat.String())

	// mjpeg without framerate (determine from input stream framerate)
	streamIn.Streams = append(streamIn.Streams, streamio.StreamInfo{
		CodecType:    "video",
		AvgFrameRate: "30000/1001",
	})
	runFFMPEGCommandFn = func(cmd *exec.Cmd, stdErrOut io.Writer) error {
		t.Equal([]string{"ffmpeg", "-f", "webm", "-i", "pipe:0", "-f", "mjpeg", "-an", "-q:v", "3", "-vf", "fps=2.997E+01", "-"}, cmd.Args)
		return nil
	}

	streamOut, errc = TranscodeStream(context.Background(), streamIn, transcodeFormat)
	ioutil.ReadAll(streamOut)
	t.NoError(<-errc)
	t.Equal("mjpeg;framerate=30000/1001", streamOut.FfmpegFormat.String())

	// mjpeg with framerate specified
	transcodeFormat = "mjpeg;framerate=5/1"
	runFFMPEGCommandFn = func(cmd *exec.Cmd, stdErrOut io.Writer) error {
		t.Equal([]string{"ffmpeg", "-f", "webm", "-i", "pipe:0", "-f", "mjpeg", "-an", "-q:v", "3", "-vf", "fps=5.000E+00", "-"}, cmd.Args)
		return nil
	}

	streamOut, errc = TranscodeStream(context.Background(), streamIn, transcodeFormat)
	ioutil.ReadAll(streamOut)
	t.NoError(<-errc)
	t.Equal("mjpeg;framerate=5/1", streamOut.FfmpegFormat.String())
}

func (t *transcodeTestSuite) TestUnsetArg() {
	args := []string{"-vc", "mjpeg", "-framerate", "5", "-an", "-q:v", "3"}
	t.Equal([]string{"-vc", "mjpeg", "-an", "-q:v", "3"}, unsetArg(args, "framerate"))
}

func (t *transcodeTestSuite) TestTranscodeStreamFailed() {
	streamIn := streamio.NewStream()
	streamIn.FfmpegFormat.Name = "webm"
	streamIn.MimeType = "video/webm"

	transcodeFormat := "mjpeg"
	runFFMPEGCommandFn = func(cmd *exec.Cmd, stdErrOut io.Writer) error {
		return fmt.Errorf("err")
	}

	_, errc := TranscodeStream(context.Background(), streamIn, transcodeFormat)
	t.Error(<-errc)
}
