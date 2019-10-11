package ingest

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/veritone/edge-stream-ingestor/api"
	apiMocks "github.com/veritone/edge-stream-ingestor/api/mocks"
	"github.com/veritone/edge-stream-ingestor/streamio"
	streamMocks "github.com/veritone/edge-stream-ingestor/streamio/mocks"
)

type segmentTestSuite struct {
	suite.Suite
	graphQLClient *apiMocks.CoreAPIClient
	cacher        *streamMocks.Cacher
	assetStore    *AssetStore
	tdo           *api.TDO
}

func (t *segmentTestSuite) SetupTest() {
	DataDirectory = "./"    // use go build temp directory
	tailConfig.Logger = nil // use default logger
	tailConfig.Poll = true

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

func (t *segmentTestSuite) TestSegmentStream() {
	// a 25 second video
	file, err := os.Open("../data/sample/video.mp4")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("mp4")
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "audio",
		Codec:     "aac",
		CodecTag:  "mp4a",
		Profile:   "LC",
	})
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "video",
		Codec:     "h264",
		CodecTag:  "avc1",
		Profile:   "High",
		Level:     13,
	})

	ctx, cancel := context.WithCancel(context.Background())
	segc, errc := segmentStream(ctx, stream, time.Second*10, true, nil, FrameExtractConfig{})

	var segments []segment
	for seg := range segc {
		segments = append(segments, seg)
	}
	cancel() // forces ffmpeg cmd to stop if it hasn't already

	if t.Len(segments, 4) {
		t.True(segments[0].initSegment)
		t.Equal(0, segments[0].index)
		t.Equal(videoSegmentMimeType, segments[0].mimeType)
		t.True(strings.HasSuffix(segments[0].filePath, "/"+initSegmentFileName))
		t.Equal("avc1.64001e,mp4a.40.2", segments[0].codecString)

		t.False(segments[1].initSegment)
		t.Equal(0, segments[1].index)
		t.Equal(videoSegmentMimeType, segments[1].mimeType)
		t.NotEmpty(segments[1].groupID)
		t.NotEmpty(segments[1].filePath)
		t.InDelta(time.Second*10, segments[1].duration, float64(time.Millisecond*100))
		t.Equal(time.Duration(0), segments[1].startOffset)
		t.Equal(segments[1].duration, segments[1].endOffset)

		t.False(segments[2].initSegment)
		t.Equal(1, segments[2].index)
		t.Equal(videoSegmentMimeType, segments[2].mimeType)
		t.NotEmpty(segments[2].groupID)
		t.NotEmpty(segments[2].filePath)
		t.InDelta(time.Second*10, segments[2].duration, float64(time.Millisecond*100))
		t.Equal(segments[1].endOffset, segments[2].startOffset)
		t.Equal(segments[1].endOffset+segments[2].duration, segments[2].endOffset)

		t.False(segments[3].initSegment)
		t.Equal(2, segments[3].index)
		t.Equal(videoSegmentMimeType, segments[3].mimeType)
		t.NotEmpty(segments[3].groupID)
		t.NotEmpty(segments[3].filePath)
		t.InDelta(time.Second*5, segments[3].duration, float64(time.Millisecond*100))
		t.Equal(segments[2].endOffset, segments[3].startOffset)
		t.Equal(segments[2].endOffset+segments[3].duration, segments[3].endOffset)
	}

	select {
	case err := <-errc:
		t.NoError(err)
	case <-time.After(time.Millisecond):
	}
}

func (t *segmentTestSuite) TestSegmentStreamNotStrict() {
	// a 25 second video
	file, err := os.Open("../data/sample/video.mp4")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("mp4")
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "audio",
		Codec:     "aac",
		CodecTag:  "mp4a",
		Profile:   "LC",
	})
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "video",
		Codec:     "h264",
		CodecTag:  "avc1",
		Profile:   "High",
		Level:     13,
	})

	ctx, cancel := context.WithCancel(context.Background())
	segc, errc := segmentStream(ctx, stream, time.Second*10, false, nil, FrameExtractConfig{})

	var segments []segment
	for seg := range segc {
		segments = append(segments, seg)
	}
	cancel() // forces ffmpeg cmd to stop if it hasn't already

	if t.Len(segments, 3) {
		t.True(segments[0].initSegment)
		t.Equal(0, segments[0].index)
		t.Equal(videoSegmentMimeType, segments[0].mimeType)
		t.True(strings.HasSuffix(segments[0].filePath, "/"+initSegmentFileName))
		t.Equal("avc1.64000d,mp4a.40.2", segments[0].codecString)

		t.False(segments[1].initSegment)
		t.Equal(0, segments[1].index)
		t.Equal(videoSegmentMimeType, segments[1].mimeType)
		t.NotEmpty(segments[1].groupID)
		t.NotEmpty(segments[1].filePath)
		t.InDelta(time.Second*16, segments[1].duration, float64(time.Second))
		t.Equal(time.Duration(0), segments[1].startOffset)
		t.Equal(segments[1].duration, segments[1].endOffset)

		t.False(segments[2].initSegment)
		t.Equal(1, segments[2].index)
		t.Equal(videoSegmentMimeType, segments[2].mimeType)
		t.NotEmpty(segments[2].groupID)
		t.NotEmpty(segments[2].filePath)
		t.InDelta(time.Second*9, segments[2].duration, float64(time.Second))
		t.Equal(segments[1].endOffset, segments[2].startOffset)
		t.Equal(segments[1].endOffset+segments[2].duration, segments[2].endOffset)
	}

	select {
	case err := <-errc:
		t.NoError(err)
	case <-time.After(time.Millisecond):
	}
}

func (t *segmentTestSuite) TestH264CodecString() {
	streamInfo := streamio.StreamInfo{
		CodecType: "video",
		Codec:     "h264",
		CodecTag:  "avc1",
		Profile:   "High",
		Level:     30,
	}

	t.Equal("avc1.64001e", h264CodecString(streamInfo))

	streamInfo.Profile = "Main"
	streamInfo.Level = 40
	t.Equal("avc1.4d0028", h264CodecString(streamInfo))

	streamInfo.Profile = "High"
	streamInfo.Level = 31
	t.Equal("avc1.64001f", h264CodecString(streamInfo))

	streamInfo.Profile = "Unknown"
	t.Equal("", h264CodecString(streamInfo))

	streamInfo.Profile = "100"
	t.Equal("avc1.64001f", h264CodecString(streamInfo))
}

func (t *segmentTestSuite) TestMP4ACodecString() {
	streamInfo := streamio.StreamInfo{
		CodecType: "audio",
		Codec:     "aac",
		CodecTag:  "mp4a",
		Profile:   "LC",
	}

	t.Equal("mp4a.40.2", mp4AudioCodecString(streamInfo))

	streamInfo.Profile = "Main"
	t.Equal("mp4a.40.1", mp4AudioCodecString(streamInfo))

	streamInfo.Profile = "Unknown"
	t.Equal("", mp4AudioCodecString(streamInfo))

	// streamInfo.Codec = "flac"
	// t.Equal("flac", mp4AudioCodecString(streamInfo))

	// streamInfo.Codec = "opus"
	// t.Equal("opus", mp4AudioCodecString(streamInfo))

	streamInfo.Codec = "mp3"
	t.Equal("", mp4AudioCodecString(streamInfo))
}

func (t *segmentTestSuite) TestGenerateCodecStrings() {
	streams := []streamio.StreamInfo{{
		CodecType: "video",
		Codec:     "h264",
		CodecTag:  "avc1",
		Profile:   "High",
		Level:     30,
	}, {
		CodecType: "audio",
		Codec:     "aac",
		CodecTag:  "mp4a",
		Profile:   "LC",
	}}

	v, a := generateCodecStrings(streams)
	t.Equal("avc1.64001e", v)
	t.Equal("mp4a.40.2", a)
}

func (t *segmentTestSuite) TestGenerateSegmentAsset() {
	ctx := context.Background()
	t.graphQLClient.On("AddMediaSegment", ctx, mock.AnythingOfType("*api.Asset")).Return(nil)

	cwd, _ := os.Getwd()
	segment := segment{
		filePath:    cwd + "/../data/sample/s00000001_2002000.m4s",
		mimeType:    "audio/mp4",
		groupID:     uuid.New(),
		index:       2,
		duration:    time.Second * 10,
		startOffset: time.Second * 10,
		endOffset:   time.Second * 20,
	}

	cfg := IngestorConfig{GenerateMediaAssets: true}
	cfg.Defaults()

	si := segmentIngestor{
		config:          cfg,
		assetStore:      t.assetStore,
		tdo:             &api.TDO{ID: "tdo1"},
		segmentDuration: time.Second * 2,
	}

	asset, err := si.generateSegmentAsset(ctx, segment)
	if t.NoError(err) {
		t.Equal(api.AssetTypeMediaSegment, asset.Type)
		t.Equal("tdo1", asset.ContainerID)
		t.Equal("audio/mp4", asset.ContentType)
		t.Equal(int64(331971), asset.Size)
		t.NotEmpty(asset.Name)
		t.Equal(map[string]interface{}{
			"segmentGroupId":     segment.groupID,
			"segmentIndex":       2,
			"segmentDurationMs":  10000,
			"segmentStartTimeMs": 10000,
			"segmentStopTimeMs":  20000,
		}, asset.Details)
	}

	t.graphQLClient.AssertNumberOfCalls(t.T(), "AddMediaSegment", 1)
}

func (t *segmentTestSuite) TestGenerateSegmentAssetInitSeg() {
	ctx := context.Background()
	t.graphQLClient.On("AddMediaSegment", ctx, mock.AnythingOfType("*api.Asset")).Return(nil)

	cwd, _ := os.Getwd()
	segment := segment{
		filePath:    cwd + "/../data/sample/init.mp4",
		mimeType:    "audio/mp4",
		groupID:     uuid.New(),
		initSegment: true,
		codecString: "abcd",
	}

	cfg := IngestorConfig{GenerateMediaAssets: true}
	cfg.Defaults()

	si := segmentIngestor{
		config:          cfg,
		assetStore:      t.assetStore,
		tdo:             &api.TDO{ID: "tdo1"},
		segmentDuration: time.Second * 2,
	}

	asset, err := si.generateSegmentAsset(ctx, segment)
	if t.NoError(err) {
		t.Equal(api.AssetTypeInitSegment, asset.Type)
		t.Equal("tdo1", asset.ContainerID)
		t.Equal("audio/mp4", asset.ContentType)
		t.Equal(int64(1425), asset.Size)
		t.NotEmpty(asset.Name)
		t.Equal(map[string]interface{}{
			"segmentGroupId":          segment.groupID,
			"codecs":                  "abcd",
			"targetSegmentDurationMs": 2000,
		}, asset.Details)
	}

	t.graphQLClient.AssertNumberOfCalls(t.T(), "AddMediaSegment", 1)
}

func (t *segmentTestSuite) TestGenerateSegmentAssetFail() {
	ctx := context.Background()
	t.graphQLClient.On("AddMediaSegment", ctx, mock.AnythingOfType("*api.Asset")).Return(nil)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		ioutil.ReadAll(r)
		return "", errors.New("Service Unavailable")
	}

	cwd, _ := os.Getwd()
	segment := segment{
		filePath:    cwd + "/../data/sample/s00000001_2002000.m4s",
		mimeType:    "audio/mp4",
		groupID:     uuid.New(),
		index:       2,
		duration:    time.Second * 10,
		startOffset: time.Second * 10,
		endOffset:   time.Second * 20,
	}

	cfg := IngestorConfig{GenerateMediaAssets: true}
	cfg.Defaults()

	si := segmentIngestor{
		config:          cfg,
		assetStore:      t.assetStore,
		tdo:             &api.TDO{ID: "tdo1"},
		segmentDuration: time.Second * 2,
	}

	_, err := si.generateSegmentAsset(ctx, segment)
	if t.Error(err) {
		t.Contains(err.Error(), "Service Unavailable")
	}
}

func (t *segmentTestSuite) TestSegmentIngestor() {
	ctx := context.Background()

	t.graphQLClient.On("AddMediaSegment", ctx, mock.AnythingOfType("*api.Asset")).Return(nil)
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk1.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk2.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk3.mp4", nil).Once()

	cfg := IngestorConfig{
		GenerateMediaAssets: true,
		SegmentDuration:     "2s",
		Chunking: ChunkingConfig{
			ChunkDuration:      "10s",
			ProduceVideoChunks: true,
		},
	}
	cfg.Defaults()

	si, err := NewSegmentIngestor(cfg, t.cacher, t.assetStore, t.tdo)
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
		t.graphQLClient.AssertNumberOfCalls(t.T(), "AddMediaSegment", 14) // init segment + 13 2-second assets

		wg.Wait() // wait for all chunks to be processed

		if t.Len(chunks, 3) {
			// chunk 1: 0 - 10 sec
			t.Equal(1, chunks[0].Index)
			t.Equal("chunk1.mp4", chunks[0].URI)
			t.Equal(videoChunkMimeType, chunks[0].MimeType)
			t.Equal(0, chunks[0].StartOffsetMS)
			t.InDelta(10000, chunks[0].EndOffsetMS, 50)

			// chunk 2: 10 - 20 sec
			t.Equal(2, chunks[1].Index)
			t.Equal("chunk2.mp4", chunks[1].URI)
			t.Equal(videoChunkMimeType, chunks[1].MimeType)
			t.Equal(chunks[0].EndOffsetMS, chunks[1].StartOffsetMS)
			t.InDelta(chunks[0].EndOffsetMS+10000, chunks[1].EndOffsetMS, 50)

			// chunk 3: 20 - 25 sec
			t.Equal(3, chunks[2].Index)
			t.Equal("chunk3.mp4", chunks[2].URI)
			t.Equal(videoChunkMimeType, chunks[2].MimeType)
			t.Equal(chunks[1].EndOffsetMS, chunks[2].StartOffsetMS)
			t.InDelta(chunks[1].EndOffsetMS+5000, chunks[2].EndOffsetMS, 50)
		}

		t.Equal(int64(1252826), si.BytesWritten())
		t.Equal(time.Millisecond*25025, si.DurationIngested())
		t.Equal(map[string]int{
			"media-init":    1,
			"media-segment": 13,
		}, si.AssetCounts())
	}
}

func (t *segmentTestSuite) TestSegmentIngestorWithOverlap() {
	ctx := context.Background()
	t.graphQLClient.On("AddMediaSegment", ctx, mock.AnythingOfType("*api.Asset")).Return(nil)
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk1.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk2.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk3.mp4", nil).Once()

	cfg := IngestorConfig{
		GenerateMediaAssets: true,
		SegmentDuration:     "2s",
		Chunking: ChunkingConfig{
			ChunkDuration:        "10s",
			ChunkOverlapDuration: "3s",
			ProduceVideoChunks:   true,
		},
	}
	cfg.Defaults()

	si, err := NewSegmentIngestor(cfg, t.cacher, t.assetStore, t.tdo)
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
		t.graphQLClient.AssertNumberOfCalls(t.T(), "AddMediaSegment", 14) // init segment + 13 2-second assets

		wg.Wait() // wait for all chunks to be processed

		if t.Len(chunks, 3) {
			expectedOverlapMS := int((time.Second * 4) / time.Millisecond)

			// chunk 1: 0 - 10 sec
			t.Equal(1, chunks[0].Index)
			t.Equal("chunk1.mp4", chunks[0].URI)
			t.Equal(videoChunkMimeType, chunks[0].MimeType)
			t.Equal(0, chunks[0].StartOffsetMS)
			t.InDelta(10000, chunks[0].EndOffsetMS, 50)

			// chunk 2: 4 - 20 sec, first 6 from overlap
			t.Equal(2, chunks[1].Index)
			t.Equal("chunk2.mp4", chunks[1].URI)
			t.Equal(videoChunkMimeType, chunks[1].MimeType)
			t.InDelta(chunks[0].EndOffsetMS-expectedOverlapMS, chunks[1].StartOffsetMS, 50)
			t.InDelta(chunks[0].EndOffsetMS+10000, chunks[1].EndOffsetMS, 50)

			// chunk 3: 14 - 25 sec, first 6 from overlap
			t.Equal(3, chunks[2].Index)
			t.Equal("chunk3.mp4", chunks[2].URI)
			t.Equal(videoChunkMimeType, chunks[2].MimeType)
			t.InDelta(chunks[1].EndOffsetMS-expectedOverlapMS, chunks[2].StartOffsetMS, 50)
			t.InDelta(chunks[1].EndOffsetMS+5000, chunks[2].EndOffsetMS, 50)
		}

		t.Equal(int64(1252826), si.BytesWritten())
		t.Equal(time.Millisecond*25025, si.DurationIngested())
		t.Equal(map[string]int{
			"media-init":    1,
			"media-segment": 13,
		}, si.AssetCounts())
	}
}

func (t *segmentTestSuite) TestSegmentIngestorWithFrameExtractor() {
	ctx := context.Background()

	t.graphQLClient.On("AddMediaSegment", ctx, mock.AnythingOfType("*api.Asset")).Return(nil)
	t.graphQLClient.On("CreateAsset", mock.Anything, mock.AnythingOfType("*api.Asset"), mock.AnythingOfType("*bytes.Reader")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		ioutil.ReadAll(r)
	}).Return(&api.Asset{
		ID:        "1234",
		Type:      api.AssetTypeThumbnail,
		URI:       "https://some.uri/1234.jpg",
		SignedURI: "https://some.uri/1234.jpg?signed",
	}, nil)

	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk1.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk2.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), videoChunkMimeType).Return("chunk3.mp4", nil).Once()
	t.cacher.On("Cache", mock.Anything, mock.AnythingOfType("*os.File"), mock.AnythingOfType("string"), "image/jpeg").Return("frame.jpg", nil)

	cfg := IngestorConfig{
		GenerateMediaAssets: true,
		SegmentDuration:     "2s",
		Chunking: ChunkingConfig{
			ChunkDuration:      "10s",
			ProduceVideoChunks: true,
			ProduceImageChunks: true,
		},
		FrameExtraction: FrameExtractConfig{
			ExtractFramesPerSec: 1.0,
			ThumbnailRate:       10.0,
		},
	}
	cfg.Defaults()

	si, err := NewSegmentIngestor(cfg, t.cacher, t.assetStore, t.tdo)
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

	var videoChunks []Object
	var frames []Object
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
			switch obj.MimeType {
			case videoChunkMimeType:
				videoChunks = append(videoChunks, *obj)
			case "image/jpeg":
				frames = append(frames, *obj)
			default:
				t.Fail("unexpected chunk mime type: " + obj.MimeType)
			}
		}
	}()

	err = si.WriteStream(ctx, stream)
	if t.NoError(err) {
		t.graphQLClient.AssertNumberOfCalls(t.T(), "AddMediaSegment", 14) // init segment + 13 2-second assets

		wg.Wait() // wait for all chunks to be processed

		if t.Len(videoChunks, 3) {
			// chunk 1: 0 - 10 sec
			t.Equal(1, videoChunks[0].Index)
			t.Equal("chunk1.mp4", videoChunks[0].URI)
			t.Equal(videoChunkMimeType, videoChunks[0].MimeType)
			t.Equal(0, videoChunks[0].StartOffsetMS)
			t.InDelta(10000, videoChunks[0].EndOffsetMS, 50)

			// chunk 2: 10 - 20 sec
			t.Equal(2, videoChunks[1].Index)
			t.Equal("chunk2.mp4", videoChunks[1].URI)
			t.Equal(videoChunkMimeType, videoChunks[1].MimeType)
			t.Equal(videoChunks[0].EndOffsetMS, videoChunks[1].StartOffsetMS)
			t.InDelta(videoChunks[0].EndOffsetMS+10000, videoChunks[1].EndOffsetMS, 50)

			// chunk 3: 20 - 25 sec
			t.Equal(3, videoChunks[2].Index)
			t.Equal("chunk3.mp4", videoChunks[2].URI)
			t.Equal(videoChunkMimeType, videoChunks[2].MimeType)
			t.Equal(videoChunks[1].EndOffsetMS, videoChunks[2].StartOffsetMS)
			t.InDelta(videoChunks[1].EndOffsetMS+5000, videoChunks[2].EndOffsetMS, 50)
		}

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

		t.Equal("https://some.uri/1234.jpg", t.tdo.ThumbnailURL)
		t.Equal(int64(1252826), si.BytesWritten())
		t.InDelta(time.Millisecond*25000, si.DurationIngested(), float64(25*time.Millisecond))
		t.Equal(map[string]int{
			"media-init":    1,
			"media-segment": 13,
			"thumbnail":     3,
		}, si.AssetCounts())
	}
}

func (t *segmentTestSuite) TestSegmentStreamChromaSubsampling420() {
	// a 2 second video with height = 995 (width x height = 1920x995)
	file, err := os.Open("../data/sample/oddHigh.AVI")
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := streamio.NewStreamFromReader(file)
	stream.SetFfmpegFormat("avi")
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "audio",
		Codec:     "pcm_s16le",
		CodecTag:  "[1][0][0][0]",
		Profile:   "",
	})
	stream.Streams = append(stream.Streams, streamio.StreamInfo{
		CodecType: "video",
		Codec:     "msrle",
		CodecTag:  "[1][0][0][0]",
		Profile:   "",
		Level:     -99,
	})

	ctx, cancel := context.WithCancel(context.Background())
	segc, errc := segmentStream(ctx, stream, time.Second*10, true, nil, FrameExtractConfig{})

	var segments []segment
	for seg := range segc {
		segments = append(segments, seg)
	}
	cancel() // forces ffmpeg cmd to stop if it hasn't already

	select {
	case err := <-errc:
		t.NoError(err)
	case <-time.After(time.Millisecond):
	}
}
