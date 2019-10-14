package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"time"

	"github.com/pborman/uuid"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/api"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
)

var Logger = log.New(os.Stderr, "[ingest] ", log.LstdFlags)
var DataDirectory = "./data"

type Ingestor interface {
	streamio.StreamWriter
	Object() *Object
	AssetCounts() map[string]int
	DurationIngested() time.Duration
}

// Object refers to an item ("chunk") with time offsets that is submitted as output
type Object struct {
	TDOID         string
	Index         int
	StartOffsetMS int
	EndOffsetMS   int
	GroupID       string
	MimeType      string
	URI           string
	Width         int
	Height        int
}

func (o *Object) generateURI(fileExt string) string {
	return fmt.Sprintf("chunks/%s/%s_%08d%s", o.TDOID, o.GroupID, o.Index, fileExt)
}

// IngestorConfig defines configuration options for an Ingestor instance
type IngestorConfig struct {
	GenerateMediaAssets bool               `json:"generateMediaAssets"`
	SaveRawAsset        bool               `json:"saveRawAsset"`
	OutputRawAsset      bool               `json:"outputRawAsset"`
	SegmentDuration     string             `json:"segmentDuration,omitempty"`
	LeaveSegmentFiles   bool               `json:"leaveSegmentFiles"`
	Chunking            ChunkingConfig     `json:"chunking,omitempty"`
	FrameExtraction     FrameExtractConfig `json:"frameExtraction,omitempty"`
	WriteTimeout        string             `json:"writeTimeout,omitempty"`
}

func (c *IngestorConfig) Defaults() {
	if c.SegmentDuration == "" {
		c.SegmentDuration = defaultSegmentDuration
	}
	if c.WriteTimeout == "" {
		c.WriteTimeout = defaultWriteTimeout
	}
	c.FrameExtraction.defaults()
}

func (c *IngestorConfig) IsAssetProducer() bool {
	return c.GenerateMediaAssets || c.SaveRawAsset
}

func (c *IngestorConfig) IsChunkProducer() bool {
	return c.OutputRawAsset || c.Chunking.ProduceAudioChunks || c.Chunking.ProduceVideoChunks || (c.Chunking.ProduceImageChunks && c.FrameExtraction.ExtractFramesPerSec > 0)
}

type ChunkingConfig struct {
	ChunkDuration        string                 `json:"chunkDuration,omitempty"`
	ChunkOverlapDuration string                 `json:"chunkOverlapDuration,omitempty"`
	ProduceAudioChunks   bool                   `json:"produceAudioChunks"`
	ProduceVideoChunks   bool                   `json:"produceVideoChunks"`
	ProduceImageChunks   bool                   `json:"produceImageChunks"`
	Cache                streamio.S3CacheConfig `json:"cache,omitempty"`
}

func (c *ChunkingConfig) parseDurations() (time.Duration, time.Duration, error) {
	var (
		chunkDuration        time.Duration
		chunkOverlapDuration time.Duration
		err                  error
	)

	if c.ChunkDuration == "" {
		return chunkDuration, chunkOverlapDuration, nil
	}

	chunkDuration, err = time.ParseDuration(c.ChunkDuration)
	if err != nil {
		return chunkDuration, chunkOverlapDuration, fmt.Errorf(`invalid config value for "chunkDuration": %q - %s`, c.ChunkDuration, err)
	}

	if c.ChunkOverlapDuration != "" {
		chunkOverlapDuration, err = time.ParseDuration(c.ChunkOverlapDuration)
		if err != nil {
			return chunkDuration, chunkOverlapDuration, fmt.Errorf(`invalid config value for "chunkOverlapDuration": %q - %s`, c.ChunkOverlapDuration, err)
		}
	}

	if chunkDuration < chunkOverlapDuration {
		err = fmt.Errorf(`invalid config values: "chunkDuration" [%s] must be greater than "chunkOverlapDuration" [%s]`, chunkDuration, chunkOverlapDuration)
	}

	return chunkDuration, chunkOverlapDuration, err
}

func RequiresSegmenter(stream *streamio.Stream, config IngestorConfig) bool {
	if stream.ContainsVideo() {
		return config.GenerateMediaAssets || config.Chunking.ProduceVideoChunks
	}
	if stream.ContainsAudio() {
		return config.GenerateMediaAssets
	}
	return false
}

func RequiresFrameExporter(stream *streamio.Stream, config IngestorConfig) bool {
	// frame extraction piggy-backs on the segmenter when used
	return stream.ContainsVideo() && !RequiresSegmenter(stream, config) && config.Chunking.ProduceImageChunks && config.FrameExtraction.ExtractFramesPerSec > 0
}

func RequiresAudioChunker(stream *streamio.Stream, config IngestorConfig) bool {
	return stream.ContainsAudio() && config.Chunking.ProduceAudioChunks
}

func RequiresRawAssetWriter(stream *streamio.Stream, config IngestorConfig) bool {
	if stream.ContainsVideo() || stream.ContainsAudio() {
		return config.SaveRawAsset
	}
	return config.GenerateMediaAssets
}

func RequiresObjectCacheWriter(stream *streamio.Stream, config IngestorConfig) bool {
	if RequiresRawAssetWriter(stream, config) {
		// RawAssetIngestor will create the asset and output it as a chunk, so we don't need to cache it
		return false
	}
	return stream.IsImage() || stream.IsText() || config.OutputRawAsset
}

type rawAssetIngestor struct {
	config     IngestorConfig
	chunkCache streamio.Cacher
	tdo        *api.TDO
	assetStore *AssetStore
	prog       *Progress
	objectc    chan *Object
}

func NewRawAssetIngestor(config IngestorConfig, assetStore *AssetStore, tdo *api.TDO) Ingestor {
	return &rawAssetIngestor{
		config:     config,
		prog:       new(Progress),
		assetStore: assetStore,
		tdo:        tdo,
		objectc:    make(chan *Object, 1),
	}
}

func (ig *rawAssetIngestor) BytesWritten() int64 {
	return ig.prog.BytesWritten()
}

func (ig *rawAssetIngestor) DurationIngested() time.Duration {
	return ig.prog.Duration()
}

func (ig *rawAssetIngestor) AssetCounts() map[string]int {
	return ig.prog.AssetCounts()
}

func (ig *rawAssetIngestor) Object() *Object {
	return <-ig.objectc
}

func (ig *rawAssetIngestor) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	defer close(ig.objectc)

	dimX, dimY := stream.Dimensions()
	obj := Object{
		TDOID:    ig.tdo.ID,
		GroupID:  uuid.New(),
		MimeType: stream.MimeType,
		Width:    dimX,
		Height:   dimY,
	}

	var assetDuration time.Duration
	var err error

	obj.URI, assetDuration, err = ig.storeStreamContents(ctx, stream)
	if err != nil {
		return fmt.Errorf("failed to create media asset from stream: %s", err)
	}

	if stream.ContainsAudio() || stream.ContainsVideo() {
		ig.prog.addDuration(assetDuration)

		// only produce raw asset as output object if configured
		if !ig.config.OutputRawAsset {
			return nil
		}

		obj.StartOffsetMS = stream.StartOffsetMS
		obj.EndOffsetMS = stream.StartOffsetMS + int(assetDuration/time.Millisecond)
	}

	ig.objectc <- &obj

	return nil
}

func (ig *rawAssetIngestor) storeStreamContents(ctx context.Context, stream *streamio.Stream) (string, time.Duration, error) {
	var dur time.Duration

	// buffer stream contents to disk (necessary for retrying uploads)
	fileBuf, err := os.Create(path.Join(os.TempDir(), uuid.New()))
	if err != nil {
		return "", dur, err
	}
	defer os.Remove(fileBuf.Name())
	defer fileBuf.Close()

	bytesRead, err := io.Copy(fileBuf, io.TeeReader(stream, ig.prog))
	// stream.Close()
	if err != nil {
		return "", dur, fmt.Errorf("failed to buffer stream: %s", err)
	}

	if bytesRead == 0 {
		return "", dur, errors.New("empty stream contents")
	}

	if _, err := fileBuf.Seek(0, io.SeekStart); err != nil {
		return "", dur, err
	}

	// check if context has been cancelled before creating asset
	select {
	case <-ctx.Done():
		return "", dur, ctx.Err()
	default:
	}

	// create asset on TDO
	asset := &api.Asset{
		ContainerID: ig.tdo.ID,
		Name:        ig.tdo.Name,
		Type:        api.AssetTypeMedia,
		ContentType: stream.MimeType,
		Size:        bytesRead,
		Details:     make(map[string]interface{}),
	}

	if stream.ContainsVideo() || stream.IsImage() {
		asset.Details["width"], asset.Details["height"] = stream.Dimensions()
	}
	if stream.ContainsAudio() || stream.ContainsVideo() {
		dur, err = determineDuration(fileBuf.Name(), stream.FfmpegFormat.Flags())
		if err != nil {
			return "", dur, fmt.Errorf("failed to determine asset duration: %s", err)
		}

		asset.Details["durationMS"] = int64(dur / time.Millisecond)
	}

	createdAsset, err := ig.assetStore.StoreAsset(ctx, asset, fileBuf, false)
	if err != nil {
		return "", dur, err
	}

	assetJSON, _ := json.Marshal(createdAsset)
	Logger.Println("raw asset saved:", string(assetJSON))

	ig.prog.addAsset(asset.Type)

	ig.tdo.PrimaryAsset = api.TDOPrimaryAsset{
		ID:        createdAsset.ID,
		AssetType: createdAsset.Type,
	}

	// set thumbnail to primary asset for images
	if stream.IsImage() {
		ig.tdo.SetThumbnailURL(createdAsset.URI)
	}

	return createdAsset.SignedURI, dur, nil
}

func determineDuration(filename string, inputFlags []string) (time.Duration, error) {
	var duration time.Duration

	args := append(inputFlags,
		"-hide_banner",
		"-i", filename,
		"-f", "null",
		"-",
	)

	cmd := exec.Command("ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		Logger.Println(string(output))
		return duration, err
	}

	re := regexp.MustCompile(` time=([0-9:.]+) `)
	scanner := bufio.NewScanner(bytes.NewReader(output))

	for scanner.Scan() {
		line := scanner.Text()

		if ok := re.MatchString(line); !ok {
			continue
		}

		matches := re.FindAllStringSubmatch(line, -1)
		if len(matches) == 0 {
			continue
		}

		subs := matches[len(matches)-1]
		if len(subs) < 2 {
			continue
		}

		timeBytes := []byte(subs[1])
		format := "03:04:05.000"

		if len(timeBytes) > len(format) {
			timeBytes = timeBytes[:len(format)] // truncate to length of format string
		}

		timeBytes = append(timeBytes, []byte(format)[len(timeBytes):]...) // pad remaining
		t, err := time.Parse(format, string(timeBytes))
		if err != nil {
			return duration, fmt.Errorf(`failed to parse duration string "%s" as time: %s`, string(timeBytes), err)
		}

		duration = time.Duration(t.Hour()) * time.Hour
		duration += time.Duration(t.Minute()) * time.Minute
		duration += time.Duration(t.Second()) * time.Second
		duration += time.Duration(t.Nanosecond()) * time.Nanosecond
	}

	return duration, nil
}
