package streamio

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gabriel-vasile/mimetype"
)

const (
	streamBufferSize = 3 * 1024 * 1024 // 3MB
	bufferPeekSize   = 2048
)

// Logger is the logger for this package
var Logger = log.New(os.Stderr, "[stream] ", log.LstdFlags)
var TextMimeTypes []string
var streamID int

var (
	// ErrUnknownMimeType is returned when the MIME-Type of stream cannot be determined
	ErrUnknownMimeType    = errors.New("could not guess mime type of stream")
	errEmptyBufferRead    = errors.New("empty buffer read")
	errStreamNotWriteable = errors.New("stream is not writeable")
)

// these mime types are not streamable and must be handled specially
var movFormats = [...]string{"mov", "mp4", "m4a", "3gp", "3g2", "mj2", "mov,mp4,m4a,3gp,3g2,mj2"}
var mimeTypeFFMEGFormatMap = map[string]string{
	"video/quicktime": "mov",
	"video/mp4":       "mp4",
	"audio/mp4":       "m4a",
	"audio/m4a":       "m4a",
	"audio/x-m4a":     "m4a",
	"video/3gpp":      "3gp",
	"video/3gpp2":     "3g2",
	"video/mj2":       "mj2",
}

const ffmpegFormatMP4 = "mp4"
const mimeTypeMP4 = "video/mp4"

var fileTypeMatchFunc = mimetype.Detect

// StreamInfo encapsulates "stream" information obtained from an ffprobe command
type StreamInfo struct {
	Index        int    `json:"index"`
	CodecType    string `json:"codec_type,omitempty"`
	Codec        string `json:"codec_name,omitempty"`
	CodecTag     string `json:"codec_tag_string,omitempty"`
	Profile      string `json:"profile,omitempty"`
	Level        int    `json:"level,omitempty"`
	Width        int    `json:"width,omitempty"`
	Height       int    `json:"height,omitempty"`
	AvgFrameRate string `json:"avg_frame_rate,omitempty"`
	SampleRate   string `json:"sample_rate,omitempty"`
	Channels     int    `json:"channels,omitempty"`
	BitDepth     int    `json:"bits_per_sample,omitempty"`
}

func (s *StreamInfo) FrameRate() float64 {
	parts := strings.Split(s.AvgFrameRate, "/")
	if len(parts) == 1 {
		n, _ := strconv.ParseFloat(parts[0], 64)
		return n
	}

	n, _ := strconv.Atoi(parts[0])
	d, _ := strconv.Atoi(parts[1])

	if d == 0 {
		return 0
	}

	return float64(n) / float64(d)
}

// Container encapsulates "container" information obtained from an ffprobe command
type Container struct {
	FormatName     string                 `json:"format_name,omitempty"`
	FormatLongName string                 `json:"format_long_name,omitempty"`
	Duration       string                 `json:"duration,omitempty"`
	Tags           map[string]interface{} `json:"tags,omitempty"`
}

// Stream contains information about an underlying readable stream and implements
// io.Reader to read from it.
type Stream struct {
	ID            int
	MimeType      string
	FfmpegFormat  Format
	StartTime     time.Time
	Streams       []StreamInfo
	Container     Container
	StartOffsetMS int
	bytesRead     *Progress
	buf           *bufio.Reader
	wr            io.WriteCloser
	errc          chan error
	donec         chan struct{}
	close         func() error
	onEOF         func() error
}

func (s *Stream) String() string {
	return fmt.Sprintf("stream:id[%d]mimeType[%s]FfmpegFormat[%+v]", s.ID, s.MimeType, s.FfmpegFormat)
}

// NewStream initializes and returns a Stream instance
func NewStream() *Stream {
	pr, pw := io.Pipe()
	stream := NewStreamFromReader(pr)
	stream.wr = pw

	return stream
}

// NewStreamFromReader initializes and returns a Stream instance that reads from r
func NewStreamFromReader(r io.Reader) *Stream {
	streamID++
	stream := &Stream{
		ID:    streamID,
		errc:  make(chan error, 100),
		donec: make(chan struct{}),
	}
	stream.readFrom(r)

	return stream
}

func (s *Stream) Read(p []byte) (n int, err error) {
	n, err = s.buf.Read(p)
	if err == io.EOF && s.onEOF != nil {
		s.onEOF()
	}
	return
}

func (s *Stream) Write(p []byte) (n int, err error) {
	if s.wr == nil {
		err = errStreamNotWriteable
		return
	}

	return s.wr.Write(p)
}

func (s *Stream) Close() error {
	Logger.Printf("[stream:%d] Close ENTER", s.ID)
	defer Logger.Printf("[stream:%d] Close EXIT", s.ID)
	if s.wr != nil {
		defer s.wr.Close()
	}
	if s.close != nil {
		return s.close()
	}

	return nil
}

func (s *Stream) OnClose(f func() error) {
	Logger.Printf("[stream:%d] OnClose ENTER", s.ID)
	defer Logger.Printf("[stream:%d] OnClose EXIT", s.ID)

	if s.close == nil {
		s.close = f
		return
	}

	f2 := s.close
	s.close = func() error {
		f2()
		return f()
	}
}

func (s *Stream) BytesRead() int64 {
	return s.bytesRead.Count()
}

func (s *Stream) Err() error {
	select {
	case err := <-s.errc:
		return err
	default:
		return nil
	}
}

func (s *Stream) ErrWait() error {
	return <-s.errc
}

func (s *Stream) SendErr(err error) {
	s.errc <- err
}

func (s *Stream) done() {
	close(s.errc)
	close(s.donec)
}

func (s *Stream) readFrom(r io.Reader) {
	s.bytesRead = new(Progress)
	r = io.TeeReader(r, s.bytesRead) // track bytes read
	s.buf = bufio.NewReaderSize(r, streamBufferSize)
}

func (s *Stream) ContainsVideo() bool {
	if s.IsImage() {
		return false
	}

	for _, stream := range s.Streams {
		if stream.CodecType != "video" {
			continue
		}

		switch stream.Codec {
		case "mjpeg", "mpng":
			// mp3 files sometimes contain embedded artwork which ffprobe interprets as a mjpeg/mpng stream - ignore if 0 framerate
			if stream.FrameRate() > 0 {
				return true
			}
		case "png", "gif", "tiff", "svg":
			// ffprobe interprets images as video streams - ignore
		default:
			return true
		}
	}

	return false
}

func (s *Stream) ContainsAudio() bool {
	for _, stream := range s.Streams {
		if stream.CodecType == "audio" {
			return true
		}
	}

	return false
}

func (s *Stream) IsMP4Format() bool {
	return strings.Contains(strings.ToLower(s.FfmpegFormat.String()), ffmpegFormatMP4) || strings.ToLower(s.MimeType) == mimeTypeMP4
}

func (s *Stream) IsImage() bool {
	return strings.HasPrefix(s.MimeType, "image/")
}

func (s *Stream) IsText() bool {
	for _, mimeType := range TextMimeTypes {
		if s.MimeType == mimeType {
			return true
		}
	}

	// other checks can go here

	return false
}

func (s *Stream) Dimensions() (int, int) {
	for _, stream := range s.Streams {
		if stream.CodecType == "video" {
			return stream.Width, stream.Height
		}
	}

	return 0, 0
}

func (s *Stream) Clone() *Stream {
	clone := NewStream()
	clone.StartTime = s.StartTime
	clone.StartOffsetMS = s.StartOffsetMS
	clone.MimeType = s.MimeType
	clone.FfmpegFormat = s.FfmpegFormat
	clone.Streams = s.Streams
	clone.Container = s.Container
	return clone
}

func (s *Stream) StreamTee() *Stream {
	r := s.buf
	w := s.Clone()
	s.readFrom(io.TeeReader(r, w))
	s.onEOF = w.Close
	return w
}

func (s *Stream) Tee(w io.Writer) {
	r := s.buf
	s.readFrom(io.TeeReader(r, w))
}

func (s *Stream) GuessMimeType() error {
	// peek head of input stream to determine mime type
	head, err := s.buf.Peek(bufferPeekSize)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error while peeking buffered input stream: %s", err)
	}

	if len(head) == 0 {
		return errEmptyBufferRead
	}

	mimeType, _ := fileTypeMatchFunc(head)

	if mimeType == "application/octet-stream" || mimeType == "inode/x-empty" {
		return ErrUnknownMimeType
	}

	s.MimeType = mimeType
	return nil
}

func (s *Stream) IsQTFormat() bool {
	if s.FfmpegFormat.Name == "" {
		if format, ok := mimeTypeFFMEGFormatMap[s.MimeType]; ok {
			s.FfmpegFormat.Name = format
		}
	}
	Logger.Println("IsQTFormat, stream=", s)

	if s.FfmpegFormat.Name != "" {
		for _, format := range movFormats {
			if s.FfmpegFormat.Name == format {
				return true
			}
		}
	}

	return false
}

func (s *Stream) StreamProfile() map[string]interface{} {
	return map[string]interface{}{
		"mimeType":      s.MimeType,
		"ffmpegFormat":  s.FfmpegFormat.String(),
		"startTime":     s.StartTime,
		"startOffsetMS": s.StartOffsetMS,
		"substreams":    s.Streams,
		// "container":     s.Container,
	}
}

func (s *Stream) SetFfmpegFormat(str string) {
	parts := strings.Split(str, ";")
	s.FfmpegFormat.Name = parts[0]
	s.FfmpegFormat.opts = parts[1:]
}

type Format struct {
	Name string
	opts []string
}

func (f *Format) String() string {
	var parts []string
	if f.Name != "" {
		parts = append(parts, f.Name)
	}
	parts = append(parts, f.opts...)
	return strings.Join(parts, ";")
}

func (f *Format) Flags() []string {
	var flags []string
	if f.Name != "" {
		flags = append(flags, "-f", f.Name)
	}

	for _, flag := range parseFlags(f.opts) {
		key, val := flag[0], flag[1]
		flags = append(flags, "-"+key)
		if val != "" {
			flags = append(flags, val)
		}
	}

	return flags
}

func (f *Format) Flag(key string) (string, bool) {
	for _, flag := range parseFlags(f.opts) {
		if flag[0] == key {
			return flag[1], true
		}
	}

	return "", false
}

func parseFlags(opts []string) [][2]string {
	var flags [][2]string
	for _, flag := range opts {
		fp := strings.SplitN(flag, "=", 2)
		key := strings.Trim(fp[0], " ")

		var value string
		if len(fp) > 1 {
			value = strings.Trim(fp[1], " ")
		}

		flags = append(flags, [2]string{key, value})
	}

	return flags
}

// Progress is used to track bytes read or written
type Progress struct {
	sync.Mutex
	n int64
}

func (pr *Progress) Write(p []byte) (n int, err error) {
	n = len(p)
	pr.Advance(n)
	return n, err
}

func (pr *Progress) Count() int64 {
	pr.Lock()
	defer pr.Unlock()
	return pr.n
}

func (pr *Progress) Advance(n int) {
	pr.Lock()
	defer pr.Unlock()
	pr.n += int64(n)
}

func (pr *Progress) Log(st time.Time) string {
	seconds := time.Since(st) / time.Second
	rateKBps := 0.0

	if seconds > 0 {
		rateKBps = float64(pr.Count()/1024) / float64(seconds)
	}

	return fmt.Sprintf("%s @ %02.2f kBps", pr, rateKBps)
}

func (pr *Progress) String() string {
	bs := datasize.ByteSize(pr.Count()) * datasize.B
	return bs.HR()
}
