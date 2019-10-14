package ingest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
)

const (
	maxScannerBufferCapacity   = 1024 * 1024 * 8 // 8MB
	numFrameWorkers            = 10
	defaultThumbnailWidth      = 480
	defaultExtractFramesPerSec = 0.5
)

var framesDirectory = filepath.Join(DataDirectory, "frames")

type FrameExtractConfig struct {
	ExtractFramesPerSec float64 `json:"extractFramesPerSec,omitempty"`
	FramesAsPNG         bool    `json:"framesAsPNG"`
	ThumbnailRate       float64 `json:"thumbnailRate,omitempty"`
	ThumbnailWidth      int     `json:"thumnailWidth,omitempty"`
}

func (c *FrameExtractConfig) defaults() {
	if c.ThumbnailWidth == 0 {
		c.ThumbnailWidth = defaultThumbnailWidth
	}
	if c.ExtractFramesPerSec == 0 {
		c.ExtractFramesPerSec = defaultExtractFramesPerSec
	}
}

type Frame struct {
	file     string
	img      image.Image
	length   int
	index    int
	width    int
	height   int
	mimeType string
}

func (f *Frame) loadImage() error {
	file, err := os.Open(f.file)
	if err != nil {
		return err
	}
	defer file.Close()

	switch f.mimeType {
	case "image/jpeg":
		// decode jpeg into image.Image
		f.img, err = jpeg.Decode(file)
	case "image/png":
		// decode png into image.Image
		f.img, err = png.Decode(file)
	default:
		return fmt.Errorf("unsupported mime type %q", f.mimeType)
	}

	return err
}

type frameExporter struct {
	config  FrameExtractConfig
	cache   streamio.Cacher
	tdoID   string
	prog    *Progress
	objectc chan Object
}

func NewFrameExporter(config FrameExtractConfig, cache streamio.Cacher, tdoID string) Ingestor {
	return &frameExporter{
		config:  config,
		cache:   cache,
		tdoID:   tdoID,
		prog:    new(Progress),
		objectc: make(chan Object, 1),
	}
}

func (f *frameExporter) BytesWritten() int64 {
	return f.prog.BytesWritten()
}

func (f *frameExporter) DurationIngested() time.Duration {
	return f.prog.Duration()
}

func (f *frameExporter) AssetCounts() map[string]int {
	return make(map[string]int)
}

func (f *frameExporter) Object() *Object {
	obj, ok := <-f.objectc
	if !ok {
		return nil
	}
	return &obj
}

func (f *frameExporter) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	defer close(f.objectc)

	if f.cache == nil {
		return fmt.Errorf("(frameExporter) Error: objectCache is empty")
	}

	durPerFrame := time.Duration(float64(time.Second) / f.config.ExtractFramesPerSec) // "duration" of each frame
	frameHandler := generateFrameHandler(f.cache, f.tdoID, f.config.ExtractFramesPerSec, stream.StartOffsetMS, f.objectc)

	return videoStreamToFrames(ctx, stream, f.config, func(ctx context.Context, frame Frame) error {
		if err := frameHandler(ctx, frame); err != nil {
			return err
		}

		f.prog.addBytesWritten(int64(frame.length))
		f.prog.addDuration(durPerFrame)

		return nil
	})
}

type handleFrameFunc func(context.Context, Frame) error

func videoStreamToFrames(ctx context.Context, stream *streamio.Stream, frameOpts FrameExtractConfig, frameHandler handleFrameFunc) error {
	fps := strconv.FormatFloat(frameOpts.ExtractFramesPerSec, 'E', -1, 64)
	ffmpegArgs := append(stream.FfmpegFormat.Flags(), // set the stream input format if available
		"-i", "pipe:0",
		"-vf", "fps="+fps, "-f", "image2pipe",
	)
	if frameOpts.FramesAsPNG {
		ffmpegArgs = append(ffmpegArgs, "-c:v", "png", "-")
	} else {
		// jpeg default
		ffmpegArgs = append(ffmpegArgs, "-qscale:v", "4" /*"-huffman", "optimal",*/, "-")
	}

	pr, pw := io.Pipe() // pipe stdout to frame extractor
	errc := make(chan error, 2)

	cmd := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs...)
	cmd.Stdin = stream
	cmd.Stdout = pw

	extractFramesFn, err := newFrameExtractor(frameOpts, frameHandler)
	if err != nil {
		return fmt.Errorf("cannot extract frames: %s", err)
	}

	go func() {
		defer close(errc)
		defer io.Copy(ioutil.Discard, pr) // exhaust the reader

		// start frame extractor on stdout pipe of the ffmpeg cmd
		if err := extractFramesFn(ctx, pr); err != nil {
			errc <- fmt.Errorf("failed to extract frames from stream: %s", err)
		}
	}()

	Logger.Printf("FFMPEG %v", ffmpegArgs)
	err = runFFMPEGCommandFn(cmd, nil)
	pw.Close()
	if err != nil {
		return err
	}

	return <-errc
}

func generateFrameHandler(objectCache streamio.Cacher, tdoID string, fps float64, offsetMS int, objectc chan<- Object) handleFrameFunc {
	msPerFrame := int(float64(time.Second)/fps) / int(time.Millisecond)

	return func(ctx context.Context, frame Frame) error {
		file, err := os.Open(frame.file)
		if err != nil {
			return err
		}
		defer file.Close()

		destPath := fmt.Sprintf("frames/%s/%s%s", tdoID, uuid.New(), path.Ext(frame.file))
		cacheLocation, err := objectCache.Cache(ctx, file, destPath, frame.mimeType)
		if err != nil {
			return fmt.Errorf("S3 upload err: %s", err)
		}

		// calculate relative start time of object using frame index and frame duration
		relStartMS := frame.index * msPerFrame
		relEndMS := (frame.index + 1) * msPerFrame
		obj := Object{
			TDOID:         tdoID,
			Index:         frame.index + 1,
			StartOffsetMS: relStartMS + offsetMS,
			EndOffsetMS:   relEndMS + offsetMS,
			URI:           cacheLocation,
			MimeType:      frame.mimeType,
			Width:         frame.width,
			Height:        frame.height,
		}

		select {
		case <-ctx.Done():
		case objectc <- obj:
		}

		return nil
	}
}

type extractFramesFunc func(context.Context, io.Reader) error

// newFrameExtractor returns a func that reads a frame stream from a reader and an error if setup failed
func newFrameExtractor(opts FrameExtractConfig, frameHandlers ...handleFrameFunc) (extractFramesFunc, error) {
	if opts.ExtractFramesPerSec <= 0 {
		return nil, errors.New("invalid fps value")
	}

	if err := os.RemoveAll(framesDirectory); err != nil {
		Logger.Println("failed to delete frames directory contents:", err)
	}

	if err := os.MkdirAll(framesDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create frames directory: %s", err)
	}

	return func(ctx context.Context, r io.Reader) error {
		frames, errc := readFrames(r, opts.ExtractFramesPerSec, opts.FramesAsPNG)
		wg := new(sync.WaitGroup)

		// start workers and send frames on work chan
		for i := 0; i < numFrameWorkers; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for frame := range frames {
					for _, fh := range frameHandlers {
						if err := fh(ctx, frame); err != nil {
							Logger.Printf("failed to handle frame at %q: %s", frame.file, err)
						}
					}

					if err := os.Remove(frame.file); err != nil {
						Logger.Printf("failed to delete temporary frame file at %q: %s", frame.file, err)
					}
				}
			}()
		}

		err := <-errc
		wg.Wait() // wait for all workers to complete

		return err
	}, nil
}

func readFrames(r io.Reader, fps float64, usePNG bool) (chan Frame, chan error) {
	errc := make(chan error)
	work := make(chan Frame, 1000)

	go func() {
		defer close(work)

		fi := 0
		frames, ferrc := extractFrames(r, usePNG)

		for {
			select {
			case f, ok := <-frames:
				if !ok {
					// frames chan closed
					errc <- nil
					return
				}

				// decode image to validate contents and determine dimensions
				br := bytes.NewReader(f)
				ext := ".jpg"
				mimeType := "image/jpeg"

				var (
					img image.Image
					err error
				)

				if usePNG {
					ext = ".png"
					mimeType = "image/png"
					if img, err = png.Decode(br); err != nil {
						Logger.Println("invalid PNG file:", err)
						continue
					}
				} else {
					if img, err = jpeg.Decode(br); err != nil {
						Logger.Println("invalid JPEG file:", err)
						continue
					}
				}

				fn := filepath.Join(framesDirectory, fmt.Sprintf("frame_%08d%s", fi, ext))
				fd, err := os.Create(fn)
				if err != nil {
					Logger.Println("failed to create file:", err)
					continue
				}

				// flush to disk to avoid running out of memory if there is a bottleneck downstream
				br = bytes.NewReader(f)
				if _, err := br.WriteTo(fd); err != nil {
					Logger.Println("failed to flush image bytes to file:", err)
					continue
				}

				work <- Frame{
					file:     fn,
					length:   len(f),
					index:    fi,
					width:    img.Bounds().Dx(),
					height:   img.Bounds().Dy(),
					mimeType: mimeType,
				}

				fi++
			case err := <-ferrc:
				errc <- err
				return
			}
		}
	}()

	return work, errc
}

func extractFrames(r io.Reader, usePNG bool) (<-chan []byte, <-chan error) {
	frames := make(chan []byte)
	errc := make(chan error, 1) // Buffered to allow the go func below to exit and close the frames channel

	go func() {
		defer close(errc)
		defer close(frames) // This will signal that we're done sending frames to the caller

		scanner := bufio.NewScanner(r)
		buf := make([]byte, maxScannerBufferCapacity)

		scanner.Buffer(buf, len(buf))

		if usePNG {
			scanner.Split(scanPNGs)
		} else {
			scanner.Split(scanJPEGs)
		}

		for scanner.Scan() {
			frame := make([]byte, len(scanner.Bytes()))
			copy(frame, scanner.Bytes())
			frames <- frame
		}

		if err := scanner.Err(); err != nil {
			errc <- fmt.Errorf("failed on scan: %s", err)
			return
		}
	}()

	return frames, errc
}

var (
	JPEGEndMagicNum  = []byte{255, 217}
	PNGFileSignature = []byte{137, 80, 78, 71, 13, 10, 26, 10}
)

func scanJPEGs(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.Index(data, JPEGEndMagicNum); i >= 0 {
		// We have a full jpeg.
		i = i + len(JPEGEndMagicNum)
		return i, data[0:i], nil
	}

	// If we're at EOF, we have a final jpeg. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data
	return 0, nil, nil
}

// scanPNGs is a split function for a Scanner that returns a png.
// Split happens when we encounter the PNG file signature.
// PNG FILE SIGNATURE
// The first eight bytes of a PNG file always contain the following values:
//    (decimal)              137  80  78  71  13  10  26  10
//    (hexadecimal)           89  50  4e  47  0d  0a  1a  0a
//    (ASCII C notation)    \211   P   N   G  \r  \n \032 \n
func scanPNGs(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	// Search for the next PNG file signature since the last one (not the last PNG file signature
	// in the byte stream) to avoid race condition where byte stream buffer fills up much faster
	// than this function is called and more than one PNG frame are combined into a single frame.
	if i := bytes.Index(data[len(PNGFileSignature):len(data)], PNGFileSignature); i >= 0 {
		// We have a full png.
		i += len(PNGFileSignature)
		return i, data[0:i], nil
	}
	// If we're at EOF, we have a final png. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data
	return 0, nil, nil
}
