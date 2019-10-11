package ingest

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"github.com/veritone/edge-stream-ingestor/streamio"
)

const (
	audioChunkMimeType  = "audio/wav"
	audioChunkExtension = ".wav"
)

type audioChunkWriter struct {
	cache                streamio.Cacher
	outputChunkDuration  time.Duration
	chunkOverlapDuration time.Duration
	tdoID                string
	prog                 *Progress
	objectc              chan *Object
}

func NewAudioChunkWriter(config IngestorConfig, cache streamio.Cacher, tdoID string) (Ingestor, error) {
	ig := audioChunkWriter{
		cache:   cache,
		tdoID:   tdoID,
		prog:    new(Progress),
		objectc: make(chan *Object, 1000),
	}

	var err error
	ig.outputChunkDuration, ig.chunkOverlapDuration, err = config.Chunking.parseDurations()
	if err != nil {
		return nil, err
	}

	return &ig, nil
}

func (ig *audioChunkWriter) BytesWritten() int64 {
	return ig.prog.BytesWritten()
}

func (ig *audioChunkWriter) DurationIngested() time.Duration {
	return ig.prog.Duration()
}

func (ig *audioChunkWriter) AssetCounts() map[string]int {
	return make(map[string]int)
}

func (ig *audioChunkWriter) Object() *Object {
	return <-ig.objectc
}

func (ig *audioChunkWriter) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	defer close(ig.objectc)

	sctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancel ffmpeg

	// split the stream into .wav segments
	segments, errc := streamToWavChunks(sctx, stream, ig.outputChunkDuration)
	segmentGroupID := uuid.New()

	// TODO use multiple workers
	for seg := range segments {
		obj := &Object{
			TDOID:         ig.tdoID,
			GroupID:       segmentGroupID,
			Index:         seg.index + 1,
			StartOffsetMS: stream.StartOffsetMS + int(seg.startOffset/time.Millisecond),
			EndOffsetMS:   stream.StartOffsetMS + int(seg.endOffset/time.Millisecond),
			MimeType:      seg.mimeType,
		}

		file, err := os.Open(seg.filePath)
		if err != nil {
			return fmt.Errorf("failed to read from segment file %q: %s", seg.filePath, err)
		}

		// TODO handle overlaps

		tr := io.TeeReader(file, ig.prog)
		obj.URI, err = ig.cache.Cache(ctx, tr, obj.generateURI(audioChunkExtension), obj.MimeType)
		file.Close()
		if err != nil {
			return err
		}

		os.Remove(file.Name())
		ig.objectc <- obj
		ig.prog.addDuration(seg.duration)
	}

	return <-errc
}

func streamToWavChunks(ctx context.Context, stream *streamio.Stream, segmentDuration time.Duration) (<-chan segment, <-chan error) {
	// Get current executable's location
	ex, _ := os.Executable()
	cmdDir := path.Dir(ex)
	listFilePath := filepath.Join(cmdDir, DataDirectory, wavSegmentListFileName)
	os.Remove(listFilePath)

	seconds := int64(segmentDuration.Seconds())
	ffmpegArgs := append(stream.FfmpegFormat.Flags(), // set the stream input format if available
		"-i", "pipe:0",
		"-f", "segment",
		"-segment_format", "wav",
		"-segment_time", strconv.FormatInt(seconds, 10), // duration for each segment in seconds
		"-reset_timestamps", "1", // reset timestamps on each segment
		"-segment_list", listFilePath, // segment list filename
		filepath.Join(DataDirectory, "s%08d.wav"), // segment file name template
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs...)
	Logger.Printf("FFMPEG %v", ffmpegArgs)

	// Set cmd working dir to current executables location.
	cmd.Dir = cmdDir
	cmd.Stdin = stream

	return captureSegmentsFromCmd(ctx, cmd, listFilePath, audioChunkMimeType, "wav")
}
