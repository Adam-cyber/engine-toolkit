package siv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pborman/uuid"
	messages "github.com/veritone/edge-messages"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/api"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/ingest"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/messaging"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
	"log"
)

var errUnsupportedStreamType = errors.New("the stream type is not supported (must contain image, text, audio or video)")

type streamIngestor struct {
	config            ingest.IngestorConfig
	writers           []streamio.StreamWriter
	progress          *counter
	segmentWriter     ingest.Ingestor
	rawAssetWriter    ingest.Ingestor
	objectCacheWriter ingest.Ingestor
	audioChunkWriter  ingest.Ingestor
	frameExporter     ingest.Ingestor
	messageClient     *messaging.Helper
}

type outputSummary struct {
	DurationIngested time.Duration
	AssetsGenerated  map[string]int
	ObjectsGenerated map[string]int
}

func initStreamIngestor(config ingest.IngestorConfig, tdo *api.TDO, messageClient *messaging.Helper,
	chunkCache streamio.Cacher, assetStore *ingest.AssetStore, writers ...streamio.StreamWriter) (*streamIngestor, error) {

	segWriter, err := ingest.NewSegmentIngestor(config, chunkCache, assetStore, tdo)
	if err != nil {
		return nil, err
	}

	audioChunkWriter, err := ingest.NewAudioChunkWriter(config, chunkCache, tdo.ID)
	if err != nil {
		return nil, err
	}

	return &streamIngestor{
		config:            config,
		writers:           writers,
		progress:          newCounter(),
		segmentWriter:     segWriter,
		audioChunkWriter:  audioChunkWriter,
		rawAssetWriter:    ingest.NewRawAssetIngestor(config, assetStore, tdo),
		objectCacheWriter: ingest.NewObjectCacheWriter(chunkCache, tdo.ID),
		frameExporter:     ingest.NewFrameExporter(config.FrameExtraction, chunkCache, tdo.ID),
		messageClient:     messageClient,
	}, nil
}

func (si *streamIngestor) BytesWritten() int64 {
	var total int64
	for _, sw := range si.writers {
		total += sw.BytesWritten()
	}
	return total
}

func (si *streamIngestor) ObjectsSent() int {
	return si.progress.Total()
}

func (si *streamIngestor) OutputSummary() map[string]interface{} {
	var maxDuration time.Duration
	assetCounter := newCounter()

	// aggregate assets generated and duration of media ingested from all ingestors
	for _, sw := range si.writers {
		ig, ok := sw.(ingest.Ingestor)
		if !ok {
			continue
		}

		// find max duration
		if dur := ig.DurationIngested(); dur > maxDuration {
			maxDuration = dur
		}

		// sum asset counts
		if counts := ig.AssetCounts(); counts != nil {
			for assetType, count := range counts {
				assetCounter.Add(assetType, count)
			}
		}
	}

	return map[string]interface{}{
		"durationIngested": maxDuration.String(),
		"assets":           assetCounter.Counts(),
		"chunks":           si.progress.Counts(),
	}
}

func (si *streamIngestor) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	// determine additional stream writers based on configuration and stream contents
	writers := si.determineStreamWriters(stream)
	si.writers = append(writers, si.writers...) // include any pre-configured writers (kafka, stdout, etc)

	// sent after the final chunk
	var chunkEOFMsg struct {
		*sync.Mutex
		messages.ChunkEOF
	}

	chunkEOFMsg.Mutex = new(sync.Mutex)
	chunkEOFMsg.ChunkEOF = messages.EmptyChunkEOF()
	chunkEOFMsg.StartTimeOffsetMS = int64(stream.StartOffsetMS)

	wg := new(sync.WaitGroup)
	errc := make(chan error, 2*len(si.writers))
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// use as flag to wait all stream writer done
	waitCh := make(chan struct{})

	// start each stream writer
	go func() {
		for i, sw := range si.writers {
			origStream := stream

			if i < len(si.writers)-1 {
				// tee stream for additional writers
				stream = stream.StreamTee()
			}

			writerName := strings.Title(strings.Replace(fmt.Sprintf("%T", sw), "*ingest.", "", 1))
			log.Printf("Starting %q on stream %d", writerName, origStream.ID)

			wg.Add(1)
			go func(sw streamio.StreamWriter) {
				defer wg.Done()
				defer func() {
					origStream.Close()
				}() // close stream
				defer io.Copy(ioutil.Discard, origStream) // exhaust stream

				startTime := time.Now()
				if err := sw.WriteStream(sctx, origStream); err != nil {
					err = fmt.Errorf("%s: %s", writerName, err)
					log.Println(err)
					errc <- err
					cancel() // cancel all other writers
					//origStream.Close()                  // close stream
					//io.Copy(ioutil.Discard, origStream) // exhaust stream
					return
				}

				log.Printf("%s finished (took %s)", writerName, time.Since(startTime))
			}(sw)

			// if the StreamWriter is an Ingestor, read each chunk (object) and publish as message
			ig, ok := sw.(ingest.Ingestor)
			if !ok {
				continue
			}

			wg.Add(1)
			go func(ig ingest.Ingestor) {
				defer wg.Done()

				for {
					// get next object from ingestor
					obj := ig.Object()
					if obj == nil {
						return
					}

					msg := messages.EmptyMediaChunk()
					msg.MimeType = obj.MimeType
					msg.ChunkIndex = int32(obj.Index)
					msg.StartOffsetMs = int32(obj.StartOffsetMS)
					msg.EndOffsetMs = int32(obj.EndOffsetMS)
					msg.CacheURI = obj.URI
					msg.Width, msg.Height = int32(obj.Width), int32(obj.Height)
					msg.ChunkUUID = uuid.New()

					err := si.messageClient.ProduceMediaChunkMessage(ctx, &msg)
					if err != nil {
						errc <- fmt.Errorf("failed to publish media chunk message: %s", err)
						return
					}

					si.progress.Add(obj.MimeType, 1)

					log.Printf("Published %q object %d into chunk queue. [Offset: %d - %d ms] [URI: %s]",
						msg.MimeType, msg.ChunkIndex, msg.StartOffsetMs, msg.EndOffsetMs, msg.CacheURI)

					// update chunk EOF summary
					chunkEOFMsg.Lock()
					startMS := math.Min(float64(chunkEOFMsg.StartTimeOffsetMS), float64(obj.StartOffsetMS))
					endMS := math.Max(float64(chunkEOFMsg.EndTimeOffsetMS), float64(obj.EndOffsetMS))
					chunkEOFMsg.StartTimeOffsetMS = int64(startMS)
					chunkEOFMsg.EndTimeOffsetMS = int64(endMS)
					chunkEOFMsg.ChunksCount++
					chunkEOFMsg.Unlock()
				}
			}(ig)
		}
		wg.Wait()
		close(waitCh)
	}()

	for {
		select {
		case err := <-errc:
			// return immediate when having error without wait all stream writer working done
			if err != nil {
				if si.ObjectsSent() == 0 {
					// chunk eof message isn't necessary
					return err
				}
				// send chunk EOF message to chunk queue
				errK := si.messageClient.ProduceChunkEOFMessage(ctx, &chunkEOFMsg.ChunkEOF)
				if errK != nil {
					return fmt.Errorf("failed to produce EOF message to chunk queue: %s", err)
				}

				chunkEOFJSON, _ := json.Marshal(chunkEOFMsg)
				log.Println("Published chunk_eof message:", string(chunkEOFJSON))
				return err
			}
			// All stream writer working done
		case <-waitCh:
			close(errc)
			if si.ObjectsSent() == 0 {
				// chunk eof message isn't necessary
				return nil
			}
			// send chunk EOF message to chunk queue
			err := si.messageClient.ProduceChunkEOFMessage(ctx, &chunkEOFMsg.ChunkEOF)
			if err != nil {
				return fmt.Errorf("failed to produce EOF message to chunk queue: %s", err)
			}

			chunkEOFJSON, _ := json.Marshal(chunkEOFMsg)
			log.Println("Published chunk_eof message:", string(chunkEOFJSON))

			return nil
		}
	}
}

func (si *streamIngestor) determineStreamWriters(stream *streamio.Stream) []streamio.StreamWriter {
	var writers []streamio.StreamWriter

	if ingest.RequiresSegmenter(stream, si.config) && si.segmentWriter != nil {
		// writes segments for media playback segments as well as video chunks and frames
		writers = append(writers, si.segmentWriter)
	}
	if ingest.RequiresFrameExporter(stream, si.config) && si.frameExporter != nil {
		// writes frames as output objects (chunks)
		writers = append(writers, si.frameExporter)
	}
	if ingest.RequiresAudioChunker(stream, si.config) && si.audioChunkWriter != nil {
		// writes audio (wav) chunks
		writers = append(writers, si.audioChunkWriter)
	}
	if ingest.RequiresRawAssetWriter(stream, si.config) && si.rawAssetWriter != nil {
		// writes the raw stream contents as a (primary) asset
		writers = append(writers, si.rawAssetWriter)
	}
	if ingest.RequiresObjectCacheWriter(stream, si.config) && si.objectCacheWriter != nil {
		// writes the raw stream contents to the chunk cache and produces it as an output chunk
		writers = append(writers, si.objectCacheWriter)
	}

	return writers
}

func newCounter() *counter {
	return &counter{
		Mutex:  new(sync.Mutex),
		counts: make(map[string]int),
	}
}

type counter struct {
	*sync.Mutex
	counts map[string]int
	total  int
}

func (c *counter) Total() int {
	c.Lock()
	defer c.Unlock()
	return c.total
}

func (c *counter) Counts() map[string]int {
	c.Lock()
	defer c.Unlock()
	return c.counts
}

func (c *counter) Add(s string, n int) {
	c.Lock()
	defer c.Unlock()

	if c.counts == nil {
		c.counts = make(map[string]int)
	}
	if _, ok := c.counts[s]; ok {
		c.counts[s] += n
	} else {
		c.counts[s] = n
	}

	c.total += n
}
