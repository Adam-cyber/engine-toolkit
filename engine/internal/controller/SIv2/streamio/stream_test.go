package streamio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestStreamIOPackage(t *testing.T) {
	suite.Run(t, new(streamTestSuite))
	suite.Run(t, new(messageTestSuite))
	suite.Run(t, new(readerTestSuite))
	suite.Run(t, new(httpTestSuite))
}

type mockWriteCloser int

func (wc *mockWriteCloser) Write(p []byte) (n int, err error) {
	n = len(p)
	*wc += mockWriteCloser(n)
	return
}

func (wc *mockWriteCloser) Close() error {
	*wc = -1
	return nil
}

type streamTestSuite struct {
	suite.Suite
}

func (t *streamTestSuite) TestStreamPipe() {
	stream := NewStream()
	defer stream.Close()

	t.NotNil(stream)
	t.NotNil(stream.wr)
	t.NotNil(stream.errc)
	t.NotNil(stream.buf)
	t.Equal(0, stream.buf.Buffered(), "expected 0 bytes in the read buffer")

	_, ok := stream.wr.(*io.PipeWriter)
	t.True(ok, "expected stream to contain a write pipe")

	testBytes := []byte("hello")
	go func() {
		_, err := stream.Write(testBytes)
		t.NoError(err)
	}()

	p := make([]byte, len(testBytes))
	nr, err := stream.Read(p)
	t.NoError(err)
	if t.Equal(len(p), nr, "expected a full read from the buffer") {
		t.Equal(testBytes, p, "expected to read %q from buffer", testBytes)
	}
}

func (t *streamTestSuite) TestStreamFromReader() {
	testBytes := []byte("hello")
	br := bytes.NewReader(testBytes)
	stream := NewStreamFromReader(br)
	defer stream.Close()

	t.NotNil(stream)
	t.Nil(stream.wr)

	nw, err := stream.Write(testBytes)
	t.Equal(errStreamNotWriteable, err)
	t.Zero(nw)

	p := make([]byte, len(testBytes))
	nr, err := stream.Read(p)
	t.NoError(err)
	if t.Equal(len(p), nr, "expected a full read from the buffer") {
		t.Equal(testBytes, p, "expected to read %q from buffer", testBytes)
	}
}

func (t *streamTestSuite) TestStreamClose() {
	stream := NewStream()
	wc := new(mockWriteCloser)
	stream.wr = wc

	err := stream.Close()
	t.NoError(err)
	t.Equal(-1, int(*wc))

	var closed bool
	stream.close = func() error {
		closed = true
		return nil
	}

	err = stream.Close()
	t.NoError(err)
	t.True(closed)
}

func (t *streamTestSuite) TestStreamReadFrom() {
	testBytes, testBytes2 := []byte("hello"), []byte("hola")
	stream := NewStreamFromReader(bytes.NewReader(testBytes))
	stream.readFrom(bytes.NewReader(testBytes2))

	p := make([]byte, len(testBytes2))
	nr, err := stream.Read(p)
	t.NoError(err)
	if t.Equal(len(p), nr, "expected a full read from the buffer") {
		t.Equal(testBytes2, p, "expected to read %q from buffer", testBytes2)
	}
}

func (t *streamTestSuite) TestStreamErr() {
	stream := NewStream()
	defer stream.Close()

	t.NoError(stream.Err())

	err := errors.New("an error")
	stream.SendErr(err)
	t.Equal(err, stream.Err())
	t.NoError(stream.Err())

	go func() {
		t.Equal(err, stream.ErrWait())
	}()

	stream.SendErr(err)
}

func (t *streamTestSuite) TestStreamContainsAudioVideo() {
	stream := NewStream()
	defer stream.Close()

	t.False(stream.ContainsAudio())
	t.False(stream.ContainsVideo())

	stream.Streams = append(stream.Streams, StreamInfo{CodecType: "audio"})
	t.True(stream.ContainsAudio())
	t.False(stream.ContainsVideo())

	stream.Streams = append(stream.Streams, StreamInfo{CodecType: "video"})
	t.True(stream.ContainsAudio())
	t.True(stream.ContainsVideo())
}

func (t *streamTestSuite) TestStreamClone() {
	stream := NewStream()
	defer stream.Close()

	stream.MimeType = "audio/mpeg"
	stream.SetFfmpegFormat("mp3")
	stream.StartOffsetMS = 1000
	stream.StartTime = time.Now()

	clone := stream.Clone()
	t.NotNil(clone)
	t.Equal(stream.MimeType, clone.MimeType)
	t.Equal(stream.FfmpegFormat, clone.FfmpegFormat)
	t.Equal(stream.StartOffsetMS, clone.StartOffsetMS)
	t.Equal(stream.StartTime, clone.StartTime)
}

func (t *streamTestSuite) TestStreamTee() {
	testBytes := []byte("hello")
	br := bytes.NewReader(testBytes)
	stream := NewStreamFromReader(br)
	defer stream.Close()

	tee := stream.StreamTee()
	go func() {
		p := make([]byte, len(testBytes))
		_, err := tee.Read(p)
		if t.NoError(err) {
			t.Equal(testBytes, p)
		}
	}()

	p := make([]byte, len(testBytes))
	nr, err := stream.Read(p)
	t.NoError(err)
	if t.Equal(len(p), nr, "expected a full read from the buffer") {
		t.Equal(testBytes, p, "expected to read %q from buffer", testBytes)
	}
}

func (t *streamTestSuite) TestTee() {
	testBytes := []byte("hello")
	br := bytes.NewReader(testBytes)
	stream := NewStreamFromReader(br)
	defer stream.Close()

	buf := new(bytes.Buffer)
	stream.Tee(buf)

	p := make([]byte, len(testBytes))
	nr, err := stream.Read(p)
	assert.NoError(t.T(), err)
	if assert.Equal(t.T(), len(p), nr, "expected a full read from the buffer") {
		assert.Equal(t.T(), testBytes, p, "expected to read %q from buffer", testBytes)
	}
	assert.Equal(t.T(), testBytes, buf.Bytes())
}

func (t *streamTestSuite) TestStreamIsQTFormat() {
	stream := Stream{MimeType: "video/mp4"}
	t.True(stream.IsQTFormat())
	t.Equal("mp4", stream.FfmpegFormat.String())

	stream = Stream{FfmpegFormat: Format{Name: "mov"}}
	t.True(stream.IsQTFormat())

	stream = Stream{FfmpegFormat: Format{Name: "mp3"}}
	t.False(stream.IsQTFormat())
}

func (t *streamTestSuite) TestStreamGuessMimeType() {
	ctx := context.Background()
	cwd, _ := os.Getwd()

	// test empty buffer
	br := bytes.NewReader([]byte{})
	stream := NewStreamFromReader(br)
	defer stream.Close()

	err := stream.GuessMimeType()
	t.Equal(errEmptyBufferRead, err)

	br = bytes.NewReader([]byte("0123456789"))
	stream.readFrom(br)

	err = stream.GuessMimeType()
	t.NoError(err)

	t.Equal("text/plain", stream.MimeType)
	t.Equal(10, stream.buf.Buffered(), "expected part of the stream to buffered")

	// reader should not have advanced
	p := make([]byte, 5)
	_, err = stream.Read(p)
	if t.NoError(err) {
		t.Equal([]byte("01234"), p)
	}

	// video/mp4
	sampleFile := cwd + "/../data/sample/video.mp4"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}
	sr := NewFileReader(file, "video/mp4", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("video/mp4", stream.MimeType)
	}

	// simulate reader error
	br = bytes.NewReader([]byte("0123456789"))
	r := iotest.TimeoutReader(iotest.HalfReader(br)) // will return timeout err on second read
	stream = NewStreamFromReader(r)
	err = stream.GuessMimeType()
	if t.Error(err) {
		t.Contains(err.Error(), iotest.ErrTimeout.Error())
	}
}

func (t *streamTestSuite) TestStreamGuessMimeTypeText() {
	// fileTypeMatchFunc = filetype.Match
	ctx := context.Background()

	cwd, _ := os.Getwd()

	// text/plain
	sampleFile := cwd + "/../data/sample/text/sample.txt"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr := NewFileReader(file, "text/plain", "")
	stream := sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("text/plain", stream.MimeType)
	}

	// text/html
	sampleFile = cwd + "/../data/sample/text/sample.html"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "text/html; charset=utf-8", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("text/html; charset=utf-8", stream.MimeType)
	}

	// application/msword
	sampleFile = cwd + "/../data/sample/text/sample.doc"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/msword", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/msword", stream.MimeType)
	}

	// docx - application/vnd.openxmlformats-officedocument.wordprocessingml.document
	sampleFile = cwd + "/../data/sample/text/sample.docx"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/vnd.openxmlformats-officedocument.wordprocessingml.document", stream.MimeType)
	}

	// pdf - application/pdf
	sampleFile = cwd + "/../data/sample/text/sample.pdf"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/pdf", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/pdf", stream.MimeType)
	}

	// application/vnd.ms-powerpoint
	sampleFile = cwd + "/../data/sample/text/sample.ppt"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/vnd.ms-powerpoint", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/vnd.ms-powerpoint", stream.MimeType)
	}

	// application/vnd.openxmlformats-officedocument.presentationml.presentation
	sampleFile = cwd + "/../data/sample/text/sample.pptx"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/vnd.openxmlformats-officedocument.presentationml.presentation", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/vnd.openxmlformats-officedocument.presentationml.presentation", stream.MimeType)
	}

	// text/rtf
	sampleFile = cwd + "/../data/sample/text/sample.rtf"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "text/rtf", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("text/rtf", stream.MimeType)
	}

	// application/vnd.ms-excel
	sampleFile = cwd + "/../data/sample/text/sample.xls"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/vnd.ms-excel", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/vnd.ms-excel", stream.MimeType)
	}

	// application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
	sampleFile = cwd + "/../data/sample/text/sample.xlsx"
	file, err = os.Open(sampleFile)
	if err != nil {
		t.T().Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	sr = NewFileReader(file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "")
	stream = sr.Stream(ctx)
	err = stream.GuessMimeType()
	if t.NoError(err) {
		t.Equal("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", stream.MimeType)
	}
}

func (t *streamTestSuite) TestStreamFFMPEGFormatFlags() {
	f := Format{Name: "s16le"}
	t.Equal([]string{"-f", "s16le"}, f.Flags())

	f.opts = []string{"acodec=pcm_s16le", "ac=1", "ar=16000"}
	t.Equal([]string{"-f", "s16le", "-acodec", "pcm_s16le", "-ac", "1", "-ar", "16000"}, f.Flags())
}

func (t *streamTestSuite) TestParseFFMPEGFormatFlags() {
	var stream Stream
	formatStr := "s16le;acodec=pcm_s16le;ac=1;ar=16000"
	stream.SetFfmpegFormat(formatStr)
	t.Equal("s16le", stream.FfmpegFormat.Name)
	t.Equal([]string{"acodec=pcm_s16le", "ac=1", "ar=16000"}, stream.FfmpegFormat.opts)
	t.Equal(formatStr, stream.FfmpegFormat.String())

	if ar, set := stream.FfmpegFormat.Flag("ar"); t.True(set) {
		t.Equal("16000", ar)
	}
}

func (t *streamTestSuite) TestStreamInfoFrameRate() {
	var info StreamInfo
	t.Equal(0.0, info.FrameRate())

	info.AvgFrameRate = "a"
	t.Equal(0.0, info.FrameRate())

	info.AvgFrameRate = "1"
	t.Equal(1.0, info.FrameRate())

	info.AvgFrameRate = "1/1"
	t.Equal(1.0, info.FrameRate())

	info.AvgFrameRate = "25/1"
	t.Equal(25.0, info.FrameRate())

	info.AvgFrameRate = "30000/1001"
	t.Equal(float64(30000)/float64(1001), info.FrameRate())
}

func (t *streamTestSuite) TestStreamType() {
	stream := Stream{
		Streams: []StreamInfo{{
			CodecType: "audio",
			Codec:     "aac",
		}},
	}

	t.True(stream.ContainsAudio())
	t.False(stream.ContainsVideo())
	t.False(stream.IsImage())

	stream.Streams = append(stream.Streams, StreamInfo{
		CodecType: "video",
		Codec:     "h264",
	})

	t.True(stream.ContainsAudio())
	t.True(stream.ContainsVideo())
	t.False(stream.IsImage())

	stream.Streams[1].Codec = "mjpeg"
	t.False(stream.ContainsVideo())
	stream.Streams[1].Codec = "mpng"
	t.False(stream.ContainsVideo())
	stream.Streams[1].AvgFrameRate = "1/0"
	t.False(stream.ContainsVideo())
	stream.Streams[1].AvgFrameRate = "25/1"
	t.True(stream.ContainsVideo())
	stream.Streams[1].Codec = "png"
	t.False(stream.ContainsVideo())
}
