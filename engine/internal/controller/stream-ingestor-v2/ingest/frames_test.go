package ingest

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/veritone/edge-stream-ingestor/streamio"
	streamMocks "github.com/veritone/edge-stream-ingestor/streamio/mocks"
)

type framesTestSuite struct {
	suite.Suite
	cacher *streamMocks.Cacher
}

func (t *framesTestSuite) SetupTest() {
	DataDirectory = "./" // use go build temp directory
	t.cacher = new(streamMocks.Cacher)
}

func (t *framesTestSuite) TestWriteStream() {
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), "image/jpeg").Return("frame.jpg", nil)

	cfg := FrameExtractConfig{ExtractFramesPerSec: 1.0}
	f := NewFrameExporter(cfg, t.cacher, "tdo1")

	// a 25 second video
	file, err := os.Open("../data/sample/video.mp4")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("mp4")
	stream.Streams = append(stream.Streams, streamio.StreamInfo{CodecType: "video", Codec: "h264"})

	var frames []Object
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			// get next object from ingestor
			obj := f.Object()
			if obj == nil {
				return
			}
			frames = append(frames, *obj)
		}
	}()

	ctx := context.Background()
	err = f.WriteStream(ctx, stream)
	if t.NoError(err) {
		wg.Wait() // wait for all chunks to be processed

		if t.Len(frames, 25) {
			// frame 1
			t.Equal(1, frames[0].Index)
			t.Equal("frame.jpg", frames[0].URI)
			t.Equal("image/jpeg", frames[0].MimeType)
			t.Equal(0, frames[0].StartOffsetMS)
			t.Equal(1000, frames[0].EndOffsetMS)

			// frame 25
			t.Equal(25, frames[24].Index)
			t.Equal("frame.jpg", frames[24].URI)
			t.Equal("image/jpeg", frames[24].MimeType)
			t.Equal(24000, frames[24].StartOffsetMS)
			t.Equal(25000, frames[24].EndOffsetMS)
		}

		t.Equal(int64(358730), f.BytesWritten())
		t.InDelta(time.Millisecond*25000, f.DurationIngested(), float64(25*time.Millisecond))
		t.Equal(map[string]int{}, f.AssetCounts())
	}
}
