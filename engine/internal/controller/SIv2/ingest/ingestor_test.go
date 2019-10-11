package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/veritone/edge-stream-ingestor/api"
	apiMocks "github.com/veritone/edge-stream-ingestor/api/mocks"
	"github.com/veritone/edge-stream-ingestor/streamio"
	streamMocks "github.com/veritone/edge-stream-ingestor/streamio/mocks"
)

func TestIngestPackage(t *testing.T) {
	suite.Run(t, new(ingestorTestSuite))
	suite.Run(t, new(segmentTestSuite))
	suite.Run(t, new(audioChunkTestSuite))
	suite.Run(t, new(objectCacheTestSuite))
	suite.Run(t, new(storageTestSuite))
	suite.Run(t, new(transcodeTestSuite))
	suite.Run(t, new(framesTestSuite))
}

type ingestorTestSuite struct {
	suite.Suite
	graphQLClient *apiMocks.CoreAPIClient
	cacher        *streamMocks.Cacher
	assetStore    *AssetStore
	tdo           *api.TDO
}

func (t *ingestorTestSuite) SetupTest() {
	DataDirectory = "./" // use go build temp directory

	t.graphQLClient = new(apiMocks.CoreAPIClient)
	t.cacher = new(streamMocks.Cacher)
	t.tdo = &api.TDO{
		ID:      "tdo1",
		Details: make(map[string]interface{}),
	}

	assetStoreCfg := AssetStorageConfig{
		MaxAttempts:      5,
		RetryInterval:    0.000001, // one microsecond
		MaxRetryInterval: defaultAssetMaxRetryInterval,
	}

	t.assetStore = &AssetStore{
		config:        assetStoreCfg,
		coreAPIClient: t.graphQLClient,
	}

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		ioutil.ReadAll(r)
		return "https://1.2.3.4/file.mp4", nil
	}
}

func (t *ingestorTestSuite) TestRawAssetIngestor() {
	ctx := context.Background()
	readBuf := new(bytes.Buffer)

	t.graphQLClient.On("CreateAsset", ctx, mock.AnythingOfType("*api.Asset"), mock.AnythingOfType("*os.File")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		b, _ := ioutil.ReadAll(r)
		readBuf.Write(b)
	}).Return(&api.Asset{
		ID:        "1234",
		Type:      api.AssetTypeMedia,
		URI:       "https://some.uri/1234.jpg",
		SignedURI: "https://some.uri/1234.jpg?signed",
	}, nil)

	testBytes := []byte("i am an image")
	br := bytes.NewReader(testBytes)
	stream := streamio.NewStreamFromReader(br)
	stream.MimeType = "image/jpeg"
	defer stream.Close()

	cfg := IngestorConfig{}
	cfg.Defaults()
	ig := NewRawAssetIngestor(cfg, t.assetStore, t.tdo)

	err := ig.WriteStream(ctx, stream)
	if t.NoError(err) {
		if t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1) {
			// createAsset
			assetArg := t.graphQLClient.Calls[0].Arguments.Get(1).(*api.Asset)
			t.Equal("image/jpeg", assetArg.ContentType)
			t.Equal(int64(13), assetArg.Size)
			t.Equal(api.AssetTypeMedia, assetArg.Type)
			t.Equal("tdo1", assetArg.ContainerID)
			t.Equal(testBytes, readBuf.Bytes())
		}

		var chunks []Object
		for {
			// get next object from ingestor
			obj := ig.Object()
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
			t.Equal("https://some.uri/1234.jpg?signed", chunks[0].URI)
		}

		t.Equal(int64(13), ig.BytesWritten())
		t.Equal(time.Duration(0), ig.DurationIngested())
		t.Equal(map[string]int{"media": 1}, ig.AssetCounts())
		t.Equal("https://some.uri/1234.jpg", t.tdo.ThumbnailURL)
	}
}

func (t *ingestorTestSuite) TestRawAssetIngestorVideoChunk() {
	ctx := context.Background()

	t.graphQLClient.On("CreateAsset", ctx, mock.AnythingOfType("*api.Asset"), mock.AnythingOfType("*os.File")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		ioutil.ReadAll(r)
	}).Return(&api.Asset{
		ID:        "1234",
		Type:      api.AssetTypeMedia,
		URI:       "https://some.uri/1234.mp4",
		SignedURI: "https://some.uri/1234.mp4?signed",
	}, nil)

	file, err := os.Open("../data/sample/video.mp4")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("mp4")
	stream.MimeType = "video/mp4"
	stream.Streams = append(stream.Streams, streamio.StreamInfo{CodecType: "audio", Codec: "aac"})
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "video",
		Codec:     "h264",
		Width:     1280,
		Height:    720,
	})

	cfg := IngestorConfig{OutputRawAsset: true}
	cfg.Defaults()
	ig := NewRawAssetIngestor(cfg, t.assetStore, t.tdo)

	err = ig.WriteStream(ctx, stream)
	if t.NoError(err) {
		if t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1) {
			// createAsset
			assetArg := t.graphQLClient.Calls[0].Arguments.Get(1).(*api.Asset)
			t.Equal("video/mp4", assetArg.ContentType)
			t.Equal(int64(729896), assetArg.Size)
			t.Equal(api.AssetTypeMedia, assetArg.Type)
			t.Equal("tdo1", assetArg.ContainerID)
		}

		var chunks []Object
		for {
			// get next object from ingestor
			obj := ig.Object()
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
			t.Equal(25020, chunks[0].EndOffsetMS)
			t.Equal("video/mp4", chunks[0].MimeType)
			t.Equal(1280, chunks[0].Width)
			t.Equal(720, chunks[0].Height)
			t.Equal("https://some.uri/1234.mp4?signed", chunks[0].URI)
		}

		t.Equal(int64(729896), ig.BytesWritten())
		t.InDelta(time.Millisecond*25000, ig.DurationIngested(), float64(25*time.Millisecond))
		t.Equal(map[string]int{"media": 1}, ig.AssetCounts())
	}
}

func (t *ingestorTestSuite) TestRawAssetIngestorMediaNoneMimeType() {
	var (
		errStr = errors.New("A content type could not be determined from the supplied asset input.Set the contentType input field to create the asset.")
	)
	ctx := context.Background()
	t.graphQLClient.On("CreateAsset", ctx, mock.AnythingOfType("*api.Asset"), mock.AnythingOfType("*os.File")).Return(nil, errStr)

	file, err := os.Open("../data/sample/video.mp4")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("mp4")
	stream.MimeType = ""
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "video",
		Codec:     "h264",
		Width:     1280,
		Height:    720,
	})

	cfg := IngestorConfig{OutputRawAsset: true}
	cfg.Defaults()
	ig := NewRawAssetIngestor(cfg, t.assetStore, t.tdo)
	err = ig.WriteStream(ctx, stream)
	if err != nil {
		errExpected := fmt.Sprintf("failed to create media asset from stream: %s", errStr.Error())
		t.Equal(errExpected, err.Error())
	}
}
