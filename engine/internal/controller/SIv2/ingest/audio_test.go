package ingest

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/veritone/edge-stream-ingestor/streamio"
	streamMocks "github.com/veritone/edge-stream-ingestor/streamio/mocks"
)

type audioChunkTestSuite struct {
	suite.Suite
	cacher *streamMocks.Cacher
}

func (t *audioChunkTestSuite) SetupTest() {
	DataDirectory = "./" // use go build temp directory
	t.cacher = new(streamMocks.Cacher)
}

func (t *audioChunkTestSuite) TestAudioChunkWriter() {
	ctx := context.Background()

	t.cacher.On("Cache", mock.Anything, mock.Anything, mock.AnythingOfType("string"), audioChunkMimeType).Run(func(args mock.Arguments) {
		r := args.Get(1).(io.Reader)
		ioutil.ReadAll(r)
	}).Return("chunk1.wav", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.Anything, mock.AnythingOfType("string"), audioChunkMimeType).Run(func(args mock.Arguments) {
		r := args.Get(1).(io.Reader)
		ioutil.ReadAll(r)
	}).Return("chunk2.wav", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.Anything, mock.AnythingOfType("string"), audioChunkMimeType).Run(func(args mock.Arguments) {
		r := args.Get(1).(io.Reader)
		ioutil.ReadAll(r)
	}).Return("chunk3.wav", nil).Once()

	cfg := IngestorConfig{
		Chunking: ChunkingConfig{
			ChunkDuration:      "10s",
			ProduceAudioChunks: true,
		},
	}
	cfg.Defaults()

	si, err := NewAudioChunkWriter(cfg, t.cacher, "tdo1")
	if err != nil {
		t.Fail(err.Error())
	}

	// a 25 second video
	file, err := os.Open("../data/sample/video.mp4")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("mp4")
	stream.Streams = append(stream.Streams, streamio.StreamInfo{CodecType: "audio", Codec: "aac"})
	stream.Streams = append(stream.Streams, streamio.StreamInfo{CodecType: "video", Codec: "h264"})

	var chunks []Object
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			// get next object from ingestor
			obj := si.Object()
			if obj == nil {
				return
			}
			chunks = append(chunks, *obj)
		}
	}()

	err = si.WriteStream(ctx, stream)
	if t.NoError(err) {
		t.cacher.AssertNumberOfCalls(t.T(), "Cache", 3)

		wg.Wait() // wait for all chunks to be processed

		if t.Len(chunks, 3) {
			// chunk 1: 0 - 10 sec
			t.Equal(1, chunks[0].Index)
			t.Equal("chunk1.wav", chunks[0].URI)
			t.Equal(audioChunkMimeType, chunks[0].MimeType)
			t.Equal(0, chunks[0].StartOffsetMS)
			t.InDelta(10000, chunks[0].EndOffsetMS, 50)

			// chunk 2: 10 - 20 sec
			t.Equal(2, chunks[1].Index)
			t.Equal("chunk2.wav", chunks[1].URI)
			t.Equal(audioChunkMimeType, chunks[1].MimeType)
			t.Equal(chunks[0].EndOffsetMS, chunks[1].StartOffsetMS)
			t.InDelta(chunks[0].EndOffsetMS+10000, chunks[1].EndOffsetMS, 50)

			// chunk 3: 20 - 25 sec
			t.Equal(3, chunks[2].Index)
			t.Equal("chunk3.wav", chunks[2].URI)
			t.Equal(audioChunkMimeType, chunks[2].MimeType)
			t.Equal(chunks[1].EndOffsetMS, chunks[2].StartOffsetMS)
			t.InDelta(chunks[1].EndOffsetMS+5000, chunks[2].EndOffsetMS, 50)
		}

		t.Equal(int64(2205930), si.BytesWritten())
		t.InDelta(time.Second*25, si.DurationIngested(), float64(time.Millisecond*25))
		t.Equal(map[string]int{}, si.AssetCounts())
	}
}
