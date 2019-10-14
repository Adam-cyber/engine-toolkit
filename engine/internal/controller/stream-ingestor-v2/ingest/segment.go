package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image/jpeg"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mezzi/tail"
	"github.com/mezzi/tail/watch"
	"github.com/nfnt/resize"
	"github.com/pborman/uuid"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/api"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
)

const (
	defaultSegmentDuration = "2s"
	hlsSegmentListFileName = "hls_segments.m3u8"
	wavSegmentListFileName = "audio_segments.m3u8"
	initSegmentFileName    = "init.mp4"
	videoSegmentMimeType   = "video/mp4"
	audioSegmentMimeType   = "audio/mp4"
	videoChunkMimeType     = "video/mp4"
	videoChunkExtension    = ".mp4"
	numAssetWorkers        = 3
	minimumChunkDurationMS = 250
	defaultWriteTimeout    = "1m"
)

var tailConfig = tail.Config{
	Follow:    true,                  // tail -f
	Poll:      true,                  // use polling over inotify (on linux, doesn't work on mac)
	MustExist: false,                 // The file doesn't exist until after ffmpeg starts executing
	ReOpen:    true,                  // ffmpeg will overwrite the file when it opens it. tail -F
	Logger:    tail.DiscardingLogger, // disable logging
}

var initSegRegexp = regexp.MustCompile(`#EXT-X-MAP:URI=\"([^\"]+)\"`)
var segmentFilenameRegexp = regexp.MustCompile(`s(\d{8})\.(m4s|wav)`)
var segmentDurationRegexp = regexp.MustCompile(`#EXTINF:([\d\.]+)`)

// adapted from https://gist.github.com/eladkarako/119e91525d34db9f61cca23b18fd62a0#file-h264_codec_string_parser-js
var h264ProfileMap = map[string]int{
	"Baseline":    66,
	"Main":        77,
	"Extended":    88,
	"High":        100,
	"High 10":     110,
	"High 4:2:2":  122,
	"High 4:4:4":  244,
	"CAVLC 4:4:4": 44,

	// profiles for SVC - Scalable Video Coding extension to H.264
	"Scalable Baseline": 83,
	"Scalable High":     86,

	// profiles for MVC - Multiview Video Coding extension to H.264
	"Stereo High":          128,
	"Multiview High":       118,
	"Multiview Depth High": 138,
}

var mpeg4AudioObjectTypeMap = map[string]int{
	"Main":     1,
	"LC":       2,
	"SSR":      3,
	"LTP":      4,
	"HE":       5,
	"Scalable": 6,
}

// segment refers to an individual fragment of an entire recording
type segment struct {
	filePath    string
	initSegment bool
	codecString string
	groupID     string
	index       int
	mimeType    string
	startOffset time.Duration
	endOffset   time.Duration
	duration    time.Duration
	errc        chan error
}

type segmentIngestor struct {
	config               IngestorConfig
	chunkCache           streamio.Cacher
	segmentDuration      time.Duration
	outputChunkDuration  time.Duration
	chunkOverlapDuration time.Duration
	produceVideoChunks   bool
	tdo                  *api.TDO
	assetStore           *AssetStore
	prog                 *Progress
	objectc              chan Object
}

func NewSegmentIngestor(config IngestorConfig, chunkCache streamio.Cacher, assetStore *AssetStore, tdo *api.TDO) (Ingestor, error) {
	var err error
	// config.Defaults()

	if config.IsAssetProducer() && assetStore == nil {
		return nil, errors.New("an asset store must be provided when generating media assets")
	}
	if config.IsChunkProducer() && chunkCache == nil {
		return nil, errors.New("a chunk cacher must be provided when generating output chunks")
	}

	si := &segmentIngestor{
		config:     config,
		chunkCache: chunkCache,
		prog:       new(Progress),
		assetStore: assetStore,
		tdo:        tdo,
		objectc:    make(chan Object, 1),
	}

	si.segmentDuration, err = time.ParseDuration(config.SegmentDuration)
	if err != nil {
		return nil, fmt.Errorf(`invalid config value for "segmentDuration": %q - %s`, config.SegmentDuration, err)
	}

	si.outputChunkDuration, si.chunkOverlapDuration, err = config.Chunking.parseDurations()
	if err != nil {
		return nil, err
	}

	if config.Chunking.ProduceVideoChunks && si.outputChunkDuration > 0 {
		si.produceVideoChunks = true
	}

	return si, nil
}

func (si *segmentIngestor) BytesWritten() int64 {
	return si.prog.BytesWritten()
}

func (si *segmentIngestor) DurationIngested() time.Duration {
	return si.prog.Duration()
}

func (si *segmentIngestor) AssetCounts() map[string]int {
	return si.prog.AssetCounts()
}

func (si *segmentIngestor) Object() *Object {
	obj, ok := <-si.objectc
	if !ok {
		return nil
	}
	return &obj
}

func (si *segmentIngestor) WriteStream(ctx context.Context, stream *streamio.Stream) error {
	var extractFramesFn extractFramesFunc
	frameOpts := si.config.FrameExtraction
	wg := new(sync.WaitGroup)

	// create frame extractor, if configured, and piggy-back on the ffmpeg segment command
	if stream.ContainsVideo() {
		var frameHandlers []handleFrameFunc
		frameToChunkFlag := si.config.Chunking.ProduceImageChunks && frameOpts.ExtractFramesPerSec > 0 && si.chunkCache != nil

		// extract frames and submit as objects
		if frameToChunkFlag {
			frameObjHandler := generateFrameHandler(si.chunkCache, si.tdo.ID, frameOpts.ExtractFramesPerSec, stream.StartOffsetMS, si.objectc)
			frameHandlers = append(frameHandlers, frameObjHandler)
		}

		// save every x frames as a thumbnail
		if si.config.GenerateMediaAssets && frameOpts.ThumbnailRate > 0 {
			// For case require segments and don't need to produce any image chunk.
			if !frameToChunkFlag {
				frameOpts.ExtractFramesPerSec = 1 / frameOpts.ThumbnailRate
			}

			numFramesPerThumbnail := int(math.Ceil(frameOpts.ThumbnailRate * frameOpts.ExtractFramesPerSec))
			thumbnailHandler := func(ctx context.Context, frame Frame) error {
				if frame.index%numFramesPerThumbnail == 0 {
					err := si.saveFrameAsThumbnail(ctx, &frame, frameOpts.ThumbnailWidth)
					if err != nil {
						return fmt.Errorf("failed to create thumbnail asset from %q: %s", frame.file, err)
					}

					si.prog.addAsset(api.AssetTypeThumbnail)
				}

				return nil
			}

			frameHandlers = append(frameHandlers, thumbnailHandler)
		}

		if len(frameHandlers) > 0 {
			frameExtractor, err := newFrameExtractor(frameOpts, frameHandlers...)
			if err != nil {
				close(si.objectc)
				return fmt.Errorf("cannot extract frames: %s", err)
			}

			wg.Add(1)
			extractFramesFn = func(ctx context.Context, r io.Reader) error {
				defer wg.Done()
				return frameExtractor(ctx, r)
			}
		}
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancel ffmpeg command

	defer close(si.objectc)
	defer wg.Wait() // wait for asset workers & frame handlers to finish before closing objects channel

	// split the stream into file segments and extract frames
	segments, errc := segmentStream(sctx, stream, si.segmentDuration, si.config.GenerateMediaAssets, extractFramesFn, frameOpts)

	// tee segments into asset uploader and chunk handler
	t1 := make(chan segment, 100)
	t2 := make(chan segment, 100)

	go func() {
		defer close(t1)
		defer close(t2)

		for seg := range segments {
			si.prog.addDuration(seg.duration)

			timeOffset := time.Duration(stream.StartOffsetMS) * time.Millisecond
			indexOffset := 0

			if timeOffset > 0 {
				i := float64(timeOffset / si.segmentDuration)
				indexOffset = 1 + int(math.Ceil(i)) // determine seg index from time per segment
			}

			if seg.initSegment || seg.duration > 0 {
				// apply relative stream time offset and index offset
				seg.startOffset += timeOffset
				seg.endOffset += timeOffset
				seg.index += indexOffset

				if si.config.GenerateMediaAssets {
					t1 <- seg
				} else {
					close(seg.errc)
				}

				t2 <- seg
			}
		}
	}()

	wTimeout, _ := time.ParseDuration(si.config.WriteTimeout)
	writeTimeout := time.NewTimer(wTimeout)
	ticker := time.NewTicker(time.Second)
	timeoutErrc := make(chan error, 1)
	generateSegmentDone := make(chan bool, numAssetWorkers)
	var written int64
	defer writeTimeout.Stop()

	if si.config.GenerateMediaAssets {

		// start asset upload workers and receive on tee'd segments chan
		for i := 0; i < numAssetWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for segment := range t1 {

					asset, err := si.generateSegmentAsset(ctx, segment)
					if err != nil {
						segment.errc <- err
						return
					}

					si.prog.addBytesWritten(asset.Size)
					si.prog.addAsset(asset.Type)
					close(segment.errc)
					writeTimeout.Reset(wTimeout)
				}
				writeTimeout.Stop()
				fmt.Println("done segment generated")
				generateSegmentDone <- true
			}()
		}
		go func() {
			workerDone := 0
			for {
				select {
				case <-ctx.Done():
					close(timeoutErrc)
					return
				case <-ticker.C:
					if written < si.BytesWritten() {
						written = si.BytesWritten()
						writeTimeout.Reset(wTimeout)
					}
				case <-writeTimeout.C:
					// We don't want to return an error and fail the task since so far SI only hang after fully
					// ingested the stream (see VTN-29645).  So we log a message and return nil so task will succeed.
					Logger.Printf("timeout: stream writer has not written any new data in %s", si.config.WriteTimeout)
					timeoutErrc <- nil
					return
				case <-generateSegmentDone:
					workerDone++
					if workerDone == numAssetWorkers {
						close(timeoutErrc)
						return
					}
				}
			}
		}()
	} else {
		close(timeoutErrc)
	}
	timeoutErr := <-timeoutErrc

	if timeoutErr != nil {
		return timeoutErr
	}

	if stream.ContainsVideo() && si.produceVideoChunks {
		dimX, dimY := stream.Dimensions()
		chunkTemplate := Object{
			TDOID:         si.tdo.ID,
			MimeType:      videoChunkMimeType,
			StartOffsetMS: stream.StartOffsetMS,
			EndOffsetMS:   stream.StartOffsetMS,
			Width:         dimX,
			Height:        dimY,
		}

		if err := si.generateVideoChunks(ctx, t2, chunkTemplate); err != nil {
			return err
		}
	} else {
		// if no video chunks are desired, just wait until assets are generated, then return
		for seg := range t2 {
			if err := <-seg.errc; err != nil {
				return fmt.Errorf("failed to generate asset for segment %d: %s", seg.index, err)
			}

			if si.config.LeaveSegmentFiles {
				continue
			}

			// delete the temporary segment files
			if err := os.Remove(seg.filePath); err != nil {
				Logger.Printf("failed to remove segment file %q: %s", seg.filePath, err)
			}
		}
	}

	return <-errc
}

func (si *segmentIngestor) generateVideoChunks(ctx context.Context, segments <-chan segment, chunkTemplate Object) error {
	var (
		done          bool
		chunkIndex    int
		segmentBuffer []segment
		err           error
	)

	currChunk := chunkTemplate

	for {
		select {
		case seg, ok := <-segments:
			if ok {
				// wait until create asset has finished; if error, return
				if err := <-seg.errc; err != nil {
					return fmt.Errorf("failed to generate asset for segment %d: %s", seg.index, err)
				}

				if seg.initSegment {
					segmentBuffer = append(segmentBuffer, seg)
					continue
				}
				if len(segmentBuffer) == 0 {
					return errors.New("missing init segment")
				}

				segmentBuffer = append(segmentBuffer, seg)
				currChunk.GroupID = seg.groupID
				currChunk.EndOffsetMS += int(seg.duration / time.Millisecond)
			}

			ratio := float64(currChunk.EndOffsetMS) / float64(si.outputChunkDuration/time.Millisecond)
			sendNextChunk := int(math.Floor(ratio)) > chunkIndex

			// this ensures the last partial chunk is sent after segment chan has closed
			if !ok {
				done = true
				sendNextChunk = len(segmentBuffer) > 1 &&
					int(math.Ceil(ratio)) > chunkIndex &&
					(currChunk.EndOffsetMS-currChunk.StartOffsetMS) >= minimumChunkDurationMS
			}

			if sendNextChunk {
				chunkIndex++
				currChunk.Index = chunkIndex

				// save chunk to chunk cache and assign URI
				cacheChunkFn := func(r io.Reader) (string, error) {
					destPath := currChunk.generateURI(videoChunkExtension)
					return si.chunkCache.Cache(ctx, r, destPath, currChunk.MimeType)
				}

				currChunk.URI, err = generateChunkFromSegments(ctx, segmentBuffer, cacheChunkFn)
				if err != nil {
					return fmt.Errorf("failed to cache media chunk: %s", err)
				}

				// find which trailing segments should be preserved for the next chunk to achieve the desired overlap
				idxOverlapStart := len(segmentBuffer)
				var overlapSegDuration time.Duration
				if si.chunkOverlapDuration > 0 {
					i := len(segmentBuffer) - 1
					for ; i > 0; i-- { // init segment excluded
						overlapSegDuration += segmentBuffer[i].duration
						if overlapSegDuration >= si.chunkOverlapDuration {
							break
						}
					}
					idxOverlapStart = i
				}

				if !si.config.LeaveSegmentFiles {
					// delete the temporary segment files
					for _, seg := range segmentBuffer[1:idxOverlapStart] {
						if err := os.Remove(seg.filePath); err != nil {
							Logger.Printf("failed to remove segment file %q: %s", seg.filePath, err)
						}
					}
				}

				si.objectc <- currChunk

				// start a new chunk window at the end of the previous one
				currChunk = Object{
					TDOID:         si.tdo.ID,
					MimeType:      currChunk.MimeType,
					StartOffsetMS: currChunk.EndOffsetMS - int(overlapSegDuration.Nanoseconds()/int64(time.Millisecond)),
					EndOffsetMS:   currChunk.EndOffsetMS,
					Width:         currChunk.Width,
					Height:        currChunk.Height,
				}

				// truncate segment buffer
				if idxOverlapStart < len(segmentBuffer) {
					segmentBuffer = append(segmentBuffer[:1], segmentBuffer[idxOverlapStart:]...) // keep only init and overlapping segments
				} else {
					segmentBuffer = segmentBuffer[:1] // keep only init segment
				}
			}
		}

		if done {
			break
		}
	}

	if !si.config.LeaveSegmentFiles {
		// delete any files left in the segment buffer
		for _, seg := range segmentBuffer {
			if err := os.Remove(seg.filePath); err != nil {
				Logger.Printf("failed to remove segment file %q: %s", seg.filePath, err)
			}
		}
	}

	return nil
}

func (si *segmentIngestor) saveFrameAsThumbnail(ctx context.Context, thumb *Frame, width int) error {
	if err := thumb.loadImage(); err != nil {
		return fmt.Errorf("failed to load frame contents from %q: %s", thumb.file, err)
	}

	// resize to thumbnail width and preserve aspect ratio
	resizedImg := resize.Resize(uint(width), 0, thumb.img, resize.Lanczos3)

	// write resized image to buffer
	writeBuf := new(bytes.Buffer)
	err := jpeg.Encode(writeBuf, resizedImg, nil)
	if err != nil {
		return fmt.Errorf("failed to encode resized jpeg: %s", err)
	}

	thumbAsset := api.Asset{
		ContainerID: si.tdo.ID,
		Name:        "thumb_" + uuid.New() + ".jpg",
		Type:        api.AssetTypeThumbnail,
		ContentType: "image/jpeg",
		Details: map[string]interface{}{
			"width":  resizedImg.Bounds().Dx(),
			"height": resizedImg.Bounds().Dy(),
		},
		Size: int64(len(writeBuf.Bytes())),
	}

	createdAsset, err := si.assetStore.StoreAsset(ctx, &thumbAsset, writeBuf, false)
	if err != nil {
		return fmt.Errorf("failed to create asset: %s", err)
	}

	return si.tdo.SetThumbnailURL(createdAsset.URI)
}

func (si *segmentIngestor) generateSegmentAsset(ctx context.Context, seg segment) (*api.Asset, error) {
	fs, err := os.Stat(seg.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat segment file %s: %s", seg.filePath, err)
	}

	fileName := path.Base(seg.filePath)
	asset := &api.Asset{
		ContainerID: si.tdo.ID,
		Type:        api.AssetTypeMediaSegment,
		ContentType: seg.mimeType,
		Size:        fs.Size(),
	}

	if seg.initSegment {
		asset.Name = fileName
		asset.Type = api.AssetTypeInitSegment
		asset.Details = map[string]interface{}{
			"segmentGroupId":          seg.groupID,
			"codecs":                  seg.codecString,
			"targetSegmentDurationMs": int(si.segmentDuration / time.Millisecond),
		}
	} else {
		asset.Name = fmt.Sprintf("%s_%08d%s", si.tdo.ID, seg.index, path.Ext(seg.filePath))
		asset.Details = map[string]interface{}{
			"segmentGroupId":     seg.groupID,
			"segmentIndex":       seg.index,
			"segmentDurationMs":  int(seg.duration / time.Millisecond),
			"segmentStartTimeMs": int(seg.startOffset / time.Millisecond),
			"segmentStopTimeMs":  int(seg.endOffset / time.Millisecond),
		}
	}

	file, err := os.Open(seg.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read from segment file %q: %s", seg.filePath, err)
	}
	defer file.Close()

	createdAsset, err := si.assetStore.StoreAsset(ctx, asset, file, true)
	if err != nil {
		return nil, fmt.Errorf("failed to store asset at %q: %s", seg.filePath, err)
	}

	if seg.initSegment {
		Logger.Printf("Created init segment from file: %s. AssetID: %s Details: %v", fileName, createdAsset.ID, createdAsset.Details)
	} else {
		Logger.Printf("Created media segment %d from file: %s. Duration: %s AssetID: %s", seg.index, fileName, seg.duration.String(), createdAsset.ID)
	}

	return createdAsset, nil
}

func segmentStream(ctx context.Context, stream *streamio.Stream, segmentDuration time.Duration, strictDuration bool, extractFrames extractFramesFunc, frameOpts FrameExtractConfig) (<-chan segment, <-chan error) {
	// Get current executable's location
	ex, _ := os.Executable()
	cmdDir := path.Dir(ex)
	listFilePath := filepath.Join(cmdDir, DataDirectory, hlsSegmentListFileName)
	os.Remove(listFilePath)

	seconds := int64(segmentDuration.Seconds())
	ffmpegArgs := append(stream.FfmpegFormat.Flags(), // set the stream input format if available
		"-i", "pipe:0",
		"-f", "hls",
		"-max_muxing_queue_size", "2048", // https://trac.ffmpeg.org/ticket/6375
		"-hls_list_size", "0", // allow any number of segments
		"-hls_time", strconv.FormatInt(seconds, 10), // duration for each segment in seconds (cuts to next keyframe after time specified, so this should match keyframe interval)
		"-hls_segment_type", "fmp4", // fragmented mp4 output (init.mp4 + .m4s files)
		"-use_localtime", "1", // allows using segment index in output filename
		// "-hls_flags", "second_level_segment_duration+second_level_segment_index", // optional: allows saving segment duration in segment filename
		"-hls_flags", "second_level_segment_index", // include segment index in segment filename
		"-hls_segment_filename", filepath.Join(DataDirectory, "s%%08d.m4s"), // name template for each segment file
		"-hls_fmp4_init_filename", initSegmentFileName, // init segment filename (relative path)
		"-sn", "-dn", // skip subtitle and data stream
	)

	vCodec, aCodec := generateCodecStrings(stream.Streams) // returns compatible h264/aac codec strings only, otherwise empty

	if aCodec != "" {
		Logger.Printf("detected compatible audio codec: %s - skipping audio transcoding", aCodec)
		// input audio stream is compatible (aac)
		// NOTE: flac and opus require experimental flag, but it doesn't seem to work with hls muxer
		ffmpegArgs = append(ffmpegArgs,
			"-c:a", "copy", // skip transcode
			// "-strict", "-2", // allow experimental audio codecs (flac, opus)
		)
	} else if stream.ContainsAudio() {
		ffmpegArgs = append(ffmpegArgs,
			"-c:a", "aac", // use aac audio codec
			"-profile:a", "aac_low", // low complexity (LC) profile
			"-b:a", "384k", // 384 kbps audio bitrate
		)
		aCodec = mp4AudioCodecString(streamio.StreamInfo{
			Codec:    "aac",
			CodecTag: "mp4a",
			Profile:  "LC",
		})
	} else {
		ffmpegArgs = append(ffmpegArgs, "-an") // skip audio stream
	}

	// if the segment durations are strictly enforced, we must transcode in order to force keyframes at each segment
	if !strictDuration && vCodec != "" {
		Logger.Printf("detected h264 video (codec: %s) - skipping video transcoding", vCodec)
		// input video stream is compatible (h264)
		ffmpegArgs = append(ffmpegArgs,
			"-c:v", "copy", // skip transcode
		)
	} else if stream.ContainsVideo() {
		ffmpegArgs = append(ffmpegArgs,
			"-c:v", "libx264", // set video codec to h264
			"-profile:v", "high", // h264 profile: High
			"-level", "30", // h264 level: 3.0
			"-sc_threshold", "0", // disable adding keyframes at scene transitions
			"-g", "500", // increase max GOP size to allow keyframes to be further apart. ex. default value is 250. at 30 fps, key frames will be at most 8.333 seconds apart (250/30) instead of 10 seconds
			// "-bf", "2", // 2 consecutive B frames
			"-coder", "1", // use CABAC
			"-force_key_frames", fmt.Sprintf("expr:gte(t,n_forced*%d)", seconds), // force keyframe every n seconds
			"-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2", // adds a 1-pixel pad to height and/or width if they are odd
			"-pix_fmt", "yuv420p", // Chroma subsampling: 4:2:0
		)
		vCodec = h264CodecString(streamio.StreamInfo{
			Codec:    "h264",
			CodecTag: "avc1",
			Profile:  "High",
			Level:    30,
		})
	} else {
		ffmpegArgs = append(ffmpegArgs, "-vn") // skip video stream
	}

	if stream.StartOffsetMS > 0 {
		// apply time offset to timecodes of output segments (source: https://stackoverflow.com/a/43660908/9206790)
		offsetSec := (time.Duration(stream.StartOffsetMS) * time.Millisecond).Seconds()
		ffmpegArgs = append(ffmpegArgs,
			"-copyts",        // copy timestamps
			"-muxdelay", "0", // Set the maximum demux-decode delay.
			"-muxpreload", "0", // Set the initial demux-decode delay.
			"-output_ts_offset", fmt.Sprintf("%.03f", offsetSec), // apply time offset (in seconds)
		)
	}

	ffmpegArgs = append(ffmpegArgs, listFilePath)

	var stdout io.WriteCloser

	// also extract frame stream, if configured
	if extractFrames != nil {
		fps := strconv.FormatFloat(frameOpts.ExtractFramesPerSec, 'E', -1, 64)
		ffmpegArgs = append(ffmpegArgs, "-vf", "fps="+fps, "-f", "image2pipe")

		if frameOpts.FramesAsPNG {
			ffmpegArgs = append(ffmpegArgs, "-c:v", "png", "-")
		} else {
			// jpeg default
			ffmpegArgs = append(ffmpegArgs, "-qscale:v", "4" /*"-huffman", "optimal",*/, "-")
		}

		pr, pw := io.Pipe()
		stdout = pw

		go func() {
			defer io.Copy(ioutil.Discard, pr) // exhaust the reader

			// start frame extractor on stdout of the ffmpeg cmd
			if err := extractFrames(ctx, pr); err != nil {
				Logger.Println("failed to extract frames from stream:", err)
			}
		}()
	}

	codecString := strings.Trim(vCodec+","+aCodec, ",")
	segmentMimeType := audioSegmentMimeType
	if stream.ContainsVideo() {
		segmentMimeType = videoSegmentMimeType
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs...)
	Logger.Printf("FFMPEG %v", ffmpegArgs)

	// Set cmd working dir to current executables location.
	cmd.Dir = cmdDir
	cmd.Stdin = stream
	if stdout != nil {
		cmd.Stdout = stdout
	}

	return captureSegmentsFromCmd(ctx, cmd, listFilePath, segmentMimeType, codecString)
}

func captureSegmentsFromCmd(ctx context.Context, cmd *exec.Cmd, listFilePath string, segmentMimeType string, codecString string) (<-chan segment, <-chan error) {
	segments := make(chan segment, 1000)
	errc := make(chan error, 1000)

	go func() {
		defer close(segments) // This will signal that we're done sending segments to the caller
		defer close(errc)

		t, err := tail.TailFile(listFilePath, tailConfig)
		if err != nil {
			errc <- fmt.Errorf("failed to tail ffmpeg output: %s", err)
			return
		}
		defer t.Cleanup()

		go func() {
			if stdout, _ := cmd.Stdout.(io.WriteCloser); stdout != nil {
				defer stdout.Close()
			}
			// if stdin, _ := cmd.Stdin.(io.ReadCloser); stdin != nil {
			// 	defer stdin.Close()
			// }
			defer t.StopAtEOF() // Once ffmpeg is done, we can stop tailing the ffmpeg output file. This will Close the t.Lines channel which will stop the loop below.

			if err := runFFMPEGCommand(cmd, ioutil.Discard); err != nil {
				errc <- err
				return
			}

			Logger.Printf("ffmpeg segmenting cmd complete (%s)", listFilePath)
			time.Sleep(watch.POLL_DURATION) // Give the tailer time to read the last line of the output file, required when using polling.
		}()

		var seg *segment
		var segmentOffset time.Duration

		segmentGroupID := uuid.New()
		cursor := 1

		for line := range t.Lines {
			// Logger.Println("> LINE", line.Num, cursor, line.Text)
			// the tailer repeats all lines each time it opens the file, so skip already processed lines
			if line.Num < cursor {
				continue
			}

			cursor++

			if matches := initSegRegexp.FindStringSubmatch(line.Text); len(matches) > 1 {
				segments <- segment{
					initSegment: true,
					filePath:    filepath.Join(cmd.Dir, DataDirectory, matches[1]),
					groupID:     segmentGroupID,
					mimeType:    segmentMimeType,
					codecString: codecString,
					errc:        make(chan error, 1),
				}
			}

			if matches := segmentDurationRegexp.FindStringSubmatch(line.Text); len(matches) > 1 {
				durationSec, err := strconv.ParseFloat(matches[1], 64)
				if err != nil {
					errc <- fmt.Errorf("failed to parse duration from segment filename: %s - %s", line.Text, err)
					return
				}

				segDuration := time.Duration(durationSec * float64(time.Second))
				segDuration = segDuration.Round(time.Millisecond)

				seg = &segment{
					groupID:     segmentGroupID,
					mimeType:    segmentMimeType,
					duration:    segDuration,
					startOffset: segmentOffset,
					endOffset:   segmentOffset + segDuration,
					errc:        make(chan error, 1),
				}

				segmentOffset += segDuration
			}

			if matches := segmentFilenameRegexp.FindStringSubmatch(line.Text); len(matches) > 1 {
				if seg == nil {
					errc <- fmt.Errorf("encountered segment file without duration: %s", line.Text)
					return
				}

				segmentIndex, err := strconv.ParseInt(matches[1], 10, 64)
				if err != nil {
					errc <- fmt.Errorf("failed to parse index from segment filename: %s - %s", line.Text, err)
					return
				}

				seg.index = int(segmentIndex)
				seg.filePath = filepath.Join(cmd.Dir, DataDirectory, line.Text)
				segments <- *seg
				seg = nil
			}
		}

		Logger.Printf("done segmenting stream - %d lines read from %s", cursor-1, listFilePath)
	}()

	return segments, errc
}

func generateChunkFromSegments(ctx context.Context, segments []segment, cacheChunkFn func(io.Reader) (string, error)) (string, error) {
	if len(segments) < 2 {
		return "", fmt.Errorf("a chunk requires at least 2 segments, %d given", len(segments))
	}

	var files []string
	for _, seg := range segments {
		files = append(files, seg.filePath)
	}

	chunkFilePath := filepath.Join(DataDirectory, "chunk.mp4")

	// re-encode stream to mp4 to reset timestamps
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-f", "mp4",
		"-i", "concat:"+strings.Join(files, "|"),
		"-c:v", "copy",
		"-an", // drop audio stream
		"-f", "mp4",
		"-y",
		chunkFilePath,
	)

	err := runFFMPEGCommandFn(cmd, nil)
	if err != nil {
		return "", err
	}

	fr, err := os.Open(chunkFilePath)
	if err != nil {
		return "", err
	}
	defer os.Remove(chunkFilePath)
	defer fr.Close()

	// save chunk to chunk cache and assign URI
	return cacheChunkFn(fr)
}

func generateCodecStrings(streams []streamio.StreamInfo) (v string, a string) {
	for _, s := range streams {
		if codec := h264CodecString(s); codec != "" {
			v = codec
		}
		if codec := mp4AudioCodecString(s); codec != "" {
			a = codec
		}
	}

	return
}

func h264CodecString(s streamio.StreamInfo) string {
	if s.Codec != "h264" {
		return ""
	}

	// sometimes the profile is reported as a number
	profile, _ := strconv.Atoi(s.Profile)
	if profile == 0 {
		var ok bool
		profile, ok = h264ProfileMap[s.Profile]
		if !ok {
			return ""
		}
	}

	// TODO set constraint bits
	return fmt.Sprintf("%s.%02x%02x%02x", s.CodecTag, profile, 0, s.Level)
}

func mp4AudioCodecString(s streamio.StreamInfo) string {
	switch s.Codec {
	case "aac":
	// case "opus", "flac":
	// 	return s.Codec
	default:
		return ""
	}

	oti, ok := mpeg4AudioObjectTypeMap[s.Profile]
	if !ok {
		return ""
	}

	return fmt.Sprintf("mp4a.40.%d", oti)
}
