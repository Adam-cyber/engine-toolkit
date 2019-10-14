package ingest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/armon/circbuf"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
)

var runFFMPEGCommandFn = runFFMPEGCommand

func TranscodeStream(ctx context.Context, streamIn *streamio.Stream, format string) (*streamio.Stream, <-chan error) {
	streamOut := streamIn.Clone()
	streamOut.SetFfmpegFormat(format)
	streamOut.MimeType = ""

	// set the stream input format if available
	ffmpegArgs := append(streamIn.FfmpegFormat.Flags(), "-i", "pipe:0")
	ffmpegArgs = append(ffmpegArgs, unsetArg(streamOut.FfmpegFormat.Flags(), "framerate")...)

	// add framerate argument for image streams
	switch streamOut.FfmpegFormat.Name {
	case "mjpeg", "mpjpeg", "image2pipe", "image2":
		var framerate float64
		framerateVal, found := streamOut.FfmpegFormat.Flag("framerate")

		for _, s := range streamIn.Streams {
			if s.CodecType != "video" {
				continue
			}

			if found {
				s.AvgFrameRate = framerateVal
			} else {
				streamOut.SetFfmpegFormat(format + ";framerate=" + s.AvgFrameRate)
			}

			// if output framerate is not specified, use input framerate
			framerate = s.FrameRate()
			break
		}

		if framerate == 0 {
			Logger.Println("Output framerate not specified and could not be determined from input framerate. Using default of 1.0.")
			framerate = 1.0
			streamOut.SetFfmpegFormat(format + ";framerate=1")
		}

		fps := strconv.FormatFloat(framerate, 'E', 3, 64)
		ffmpegArgs = append(ffmpegArgs, "-an", "-q:v", "3", "-vf", "fps="+fps)
	}

	ffmpegArgs = append(ffmpegArgs, "-")
	Logger.Printf("FFMPEG %v", ffmpegArgs)

	cmd := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs...)
	cmd.Stdin = streamIn
	cmd.Stdout = streamOut // write stdout to outgoing stream

	errc := make(chan error)

	go func() {
		defer close(errc)
		defer streamOut.Close()
		if err := runFFMPEGCommandFn(cmd, nil); err != nil {
			errc <- err
		}
		Logger.Println("ffmpeg transcode cmd done")
	}()

	return streamOut, errc
}

func unsetArg(args []string, key string) []string {
	for i, arg := range args {
		if arg != "-"+key {
			continue
		}
		if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			args = append(args[:i], args[i+2:]...)
			break
		}
	}
	return args
}

func runFFMPEGCommand(cmd *exec.Cmd, stdErrOut io.Writer) error {
	stderrBuf, err := circbuf.NewBuffer(2 * 1024)
	if err != nil {
		return fmt.Errorf("failed to initialize stderr buffer: %s", err)
	}

	if stdErrOut != nil {
		cmd.Stderr = io.MultiWriter(stdErrOut, stderrBuf)
	} else {
		cmd.Stderr = stderrBuf
	}

	if err := cmd.Run(); err != nil {
		if err.Error() == "signal: killed" {
			return err
		}

		Logger.Println("-------------------------------------------")
		Logger.Println("ffmpeg err:", err)
		Logger.Println("------------------output-------------------")
		Logger.Println(string(stderrBuf.Bytes()))
		Logger.Println("------------------output-------------------")

		errText := err.Error()

		if errText == "exit status 1" {
			// scan to last line of stderr buffer and use for error message
			br := bytes.NewReader(stderrBuf.Bytes())
			scanner := bufio.NewScanner(br)
			for scanner.Scan() {
				errText = scanner.Text()
			}
		}

		return fmt.Errorf("ffmpeg cmd failed: %s", errText)
	}

	// ffmpeg does not exit with a non-zero exit code when it fails to get data from the stream, so parse the error here
	re := regexp.MustCompile("Nothing was written into output file ([^\n\r]*)")
	stderrBytes := stderrBuf.Bytes()

	if re.Match(stderrBytes) {
		if matches := re.FindAll(stderrBytes, -1); len(matches) > 0 {
			return fmt.Errorf("ffmpeg cmd failed: %s", matches[0])
		}
	}

	return nil
}
