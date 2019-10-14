package ingest

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/veritone/edge-stream-ingestor/streamio"
	streamMocks "github.com/veritone/edge-stream-ingestor/streamio/mocks"
)

type objectCacheTestSuite struct {
	suite.Suite
	cacher *streamMocks.Cacher
}

func (t *objectCacheTestSuite) SetupTest() {
	DataDirectory = "./" // use go build temp directory
	t.cacher = new(streamMocks.Cacher)
}

func (t *objectCacheTestSuite) TestObjectCacheWriter() {
	ctx := context.Background()
	t.cacher.On("Cache", mock.Anything, mock.Anything, mock.AnythingOfType("string"), "image/jpeg").Run(func(args mock.Arguments) {
		r := args.Get(1).(io.Reader)
		ioutil.ReadAll(r)
	}).Return("image.jpg", nil)

	testBytes := []byte("i am an image")
	br := bytes.NewReader(testBytes)
	stream := streamio.NewStreamFromReader(br)
	stream.MimeType = "image/jpeg"
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "video",
		Codec:     "jpeg",
		Width:     1280,
		Height:    720,
	})
	defer stream.Close()

	cfg := IngestorConfig{}
	cfg.Defaults()
	ocw := NewObjectCacheWriter(t.cacher, "tdo1")

	err := ocw.WriteStream(ctx, stream)
	if t.NoError(err) {
		var chunks []Object
		for {
			// get next object from ingestor
			obj := ocw.Object()
			if obj == nil {
				break
			}
			chunks = append(chunks, *obj)
		}

		if t.Len(chunks, 1) {
			t.Equal("tdo1", chunks[0].TDOID)
			t.Equal(0, chunks[0].Index)
			t.NotEqual("", chunks[0].GroupID)
			t.Equal(0, chunks[0].StartOffsetMS)
			t.Equal(0, chunks[0].EndOffsetMS)
			t.Equal("image/jpeg", chunks[0].MimeType)
			t.Equal(1280, chunks[0].Width)
			t.Equal(720, chunks[0].Height)
			t.Equal("image.jpg", chunks[0].URI)
		}

		t.Equal(int64(13), ocw.BytesWritten())
		t.Equal(time.Duration(0), ocw.DurationIngested())
		t.Equal(map[string]int{}, ocw.AssetCounts())
	}
}
