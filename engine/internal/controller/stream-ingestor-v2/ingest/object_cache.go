package ingest

import (
	"context"
	"fmt"
	"io"
	"mime"
	"time"

	"github.com/pborman/uuid"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
)

type objectCacheWriter struct {
	cache   streamio.Cacher
	id      string
	prog    *Progress
	objectc chan *Object
}

func NewObjectCacheWriter(cache streamio.Cacher, id string) Ingestor {
	return &objectCacheWriter{
		cache:   cache,
		id:      id,
		prog:    new(Progress),
		objectc: make(chan *Object, 1),
	}
}

func (ig *objectCacheWriter) BytesWritten() int64 {
	return ig.prog.BytesWritten()
}

func (ig *objectCacheWriter) DurationIngested() time.Duration {
	return ig.prog.Duration()
}

func (ig *objectCacheWriter) AssetCounts() map[string]int {
	return make(map[string]int)
}

func (ig *objectCacheWriter) Object() *Object {
	return <-ig.objectc
}

func (ig *objectCacheWriter) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	defer close(ig.objectc)

	dimX, dimY := stream.Dimensions()
	obj := Object{
		TDOID:    ig.id,
		GroupID:  uuid.New(),
		MimeType: stream.MimeType,
		Width:    dimX,
		Height:   dimY,
	}

	// determine ext from mime type
	var fileExt string
	exts, err := mime.ExtensionsByType(stream.MimeType)
	if err != nil {
		Logger.Printf("failed to determine file extension of %q mime type: %s", stream.MimeType, err)
	} else if len(exts) > 0 {
		fileExt = exts[0]
	}

	// save stream contents to obj cache and assign URI
	destPath := obj.generateURI(fileExt)

	tr := io.TeeReader(stream, ig.prog)
	obj.URI, err = ig.cache.Cache(ctx, tr, destPath, stream.MimeType)
	stream.Close()
	if err != nil {
		return err
	}

	Logger.Println("raw asset cached to:", obj.URI)

	if stream.ContainsAudio() || stream.ContainsVideo() {
		// TODO tee stream to optimize this
		dur, err := determineDuration(obj.URI, stream.FfmpegFormat.Flags())
		if err != nil {
			return fmt.Errorf("failed to determine chunk duration: %s", err)
		}

		ig.prog.addDuration(dur)
		obj.StartOffsetMS = stream.StartOffsetMS
		obj.EndOffsetMS = stream.StartOffsetMS + int(dur/time.Millisecond)
	}

	ig.objectc <- &obj

	return nil
}
