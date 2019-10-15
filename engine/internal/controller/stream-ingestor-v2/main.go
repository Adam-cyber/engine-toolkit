package siv2

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	messages "github.com/veritone/edge-messages"

	controllerClient "github.com/veritone/realtime/modules/controller/client"

	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/api"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/ingest"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/messaging"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
	"github.com/veritone/engine-toolkit/engine/internal/controller/worker"

	"encoding/json"
//	"github.com/google/uuid"
)

const (
	appName          = "SIv2"
	maxAttempts      = 5
	videoTDOMimeType = "video/mp4"
	audioTDOMimeType = "audio/mp4"
)

var (
	errMissedRecordingWindow = errors.New("we've overshot our recording window completely")
	errMissingEndTime        = errors.New("recording window must have an end time")
	sleepFunc                = time.Sleep
	intialRetryInterval      = time.Second
	rtspURLRegexp            = regexp.MustCompile("(?i)^rtsp:/")
	httpURLRegexp            = regexp.MustCompile("(?i)^https?:/")
)

type internalEngine struct {
	engineInstanceId string
	workItem         *controllerClient.EngineInstanceWorkItem
	workItemStatus   *controllerClient.TaskStatusUpdate
	statusLock       *sync.Mutex

	payload *enginePayload
	config  *engineConfig

	apiToken    string
	kafkaClient messaging.Client
}

var (
	logger        = log.New(os.Stderr, "[ main ] ", log.LstdFlags)
	transcodeFn   = ingest.TranscodeStream
	probeFn       = streamio.ProbeStream
	hb            *heartbeat
	graphQLClient api.CoreAPIClient
)

func NewStreamIngestor(payloadJSON string,
	engineInstanceId string,
	workItem *controllerClient.EngineInstanceWorkItem,
	workItemStatus *controllerClient.TaskStatusUpdate,
	graphQlTimeoutDuration string,
	statusLock *sync.Mutex) (res worker.Worker, err error) {

	method := fmt.Sprintf("[siV2:%s]", engineInstanceId)
	config, payload, err := loadConfigAndPayload(payloadJSON, workItem.EngineId, engineInstanceId, graphQlTimeoutDuration)
	if err != nil {
		// TODO better error ...
		statusLock.Lock()
		workItemStatus.TaskStatus = "failed"
		workItemStatus.ErrorCount++
		workItemStatus.FailureReason = string(messages.FailureReasonInternalError)
		workItemStatus.TaskOutput = map[string]interface{}{"error": fmt.Sprintf("Failed to load config and payload, err=%v", err)}
		statusLock.Unlock()
		log.Printf("%s failed to load payload or config, err=%v", method, err)
		return nil, errors.Wrapf(err, "%s failed in loading payload/config", method)
	}
	config.applyOverrides(*payload)

	log.Printf("config=%s", config)
	log.Printf("payload=%s", payload)

	// payload token overrides token from environment variable
	apiToken := os.Getenv("VERITONE_API_TOKEN")
	if payload.Token != "" {
		apiToken = payload.Token
	}

	// for now this is needed to send heartbeats?
	// so keep it here until we move to the controller's work loop
	config.Messaging.Kafka.ClientIDPrefix = appName + "_"
	kafkaClient, err := messaging.NewKafkaClient(config.Messaging.Kafka)
	if err != nil {
		statusLock.Lock()
		workItemStatus.ErrorCount++ // do we need to care for now?
		statusLock.Unlock()
		log.Printf("%s got error in establishing Kafka client -- ignore for now. Err=%v", method, err)
	}

	return &internalEngine{
		engineInstanceId: engineInstanceId,
		workItem:         workItem,
		workItemStatus:   workItemStatus,
		statusLock:       statusLock,
		apiToken:         apiToken,
		payload:          payload,
		config:           config,
		kafkaClient:      kafkaClient,
	}, nil
}

/**
the idea for SIv2 == KISS (Keep It Simple Stupid)
TaskPayload should spell out what the engine needs to do.

1) as a trancoder --> ffmpegOptions in payload to determine how to transcoe
2) for producing audio or video chunks
3) For producing segments for playback

** BEWARE Of other mode that SI has to do such as setting the startTime for TDOs when ingested from say tv-and-radio

payload :
     action:  transcode, chunking, playback
     ffmpegOptions: command line args for ffmpeg, e.g.
            -i {inputfile} -ac ... {outputfile}
		  which siv2 then will invoke with `ffmpeg` with the final CLI args where the inputfile, outputfile will be adjusted accordingly
     inputIOMode / outputIOMode

	   when action == transcode:
            If the inputIOMode == stream:  SIv2 is to consume a stream from the input --> run ffmpeg command with the options --> produce stream as output
            If the inputIOMode == chunk: SIv2 is to consume chunks from the input -> run ffmpeg command with the options --> produce chunks

       When action == chunking:
            SIv2 is to consume a stream --> run ffmpeg command with the ffmpegOptions --> produce chunks

       When action == playback
            SIv2 is to consume a stream --> run ffmpeg command with the ffmpegOptions --> produce segments to be stored as assets in the TDO for playback



       Most combinations of input/outputIOMode are valid with the exception out  input chunk, output stream...
             since there's no guaranteed that the chunks are in order
             and thus the output stream may be invalid due to timing issues.


NOTE:  we need to support segment, chunk first!!

*/

func (e *internalEngine) Run() (errReason worker.ErrorReason) {
	method := fmt.Sprintf("[Siv2.Run:%s,%s,%s]", e.workItem.EngineId, e.engineInstanceId, e.workItem.TaskId)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() {
		if e.kafkaClient != nil {
			e.kafkaClient.Close()
		}
	}()
	// Create messaging helper - used for heartbeats and stream writer
	messagingClient := messaging.NewHelper(e.kafkaClient, e.config.Messaging, messaging.MessageContext{
		JobID:      e.payload.JobID,
		TaskID:     e.payload.TaskID,
		TDOID:      e.payload.TDOID,
		EngineID:   e.config.EngineID,
		InstanceID: e.config.EngineInstanceID,
	})

	// start engine status heartbeats
	// TODO consolidate this into engine-toolkit
	heartbeatInterval, _ := time.ParseDuration(e.config.HeartbeatInterval)
	hb = startHeartbeat(ctx, messagingClient, heartbeatInterval)

	defer func() {
		// send final heartbeat
		status := messages.EngineStatusDone

		log.Println("Sending final heartbeat. Status:", string(status))
		if err := hb.sendHeartbeat(ctx, status, worker.ErrorReason{}, lastStreamInfo); err != nil {
			log.Println("Failed to send final heartbeat:", err)
		}
	}()
	var (
		sr  streamio.Streamer
		sw  streamio.StreamWriter
		tdo *api.TDO
	)
	// get the taskIO for output stream
	// have to find the output
	inputScfsArr, err := scfsio.GetIOForWorkItem(e.workItem, "input")
	if err != nil {
		return worker.ErrorReason{
			Err:           errors.Wrapf(err, "%s failed to get Input IO", method),
			FailureReason: messages.FailureReasonInternalError,
		}
	}
	sr = scfsio.NewSCFSIOReader(
		fmt.Sprintf("Reader:", inputScfsArr[0].ScfsIO.GetPath()),
		inputScfsArr[0].ScfsIO)
	/** TODO
	 //RIGHT NOW WE"ll do the hls etc first since that put the medis in the tdo!
	 outputScfsArr, err := scfsio.GetIOForWorkItem(e.workItem, "output")
	 if err != nil {
		 return worker.ErrorReason{
			 Err: errors.Wrapf(err, "%s failed to get Output IO", method),
			 FailureReason: messages.FailureReasonInternalError,
		 }
	 }
	*/
	e.config.MediaStartTime = scfsio.GetMediaStartTime(e.workItem)
	if e.payload.URL != "" { // offline ingestion
		log.Println("reading stream from URL:", e.payload.URL)
		sr = streamio.NewHTTPStreamer(streamio.DefaultHTTPClient, e.payload.URL)
	}

	if e.config.IsAssetProducer() || e.config.IsChunkProducer() {
		var chunkCache streamio.Cacher
		var assetStore *ingest.AssetStore

		tdoArg := &api.TDO{
			ID:      e.payload.TDOID,
			Details: make(map[string]interface{}),
		}

		// set up asset store if generating assets
		if e.config.IsAssetProducer() {
			// payload token overrides token from environment variable
			apiToken := os.Getenv("VERITONE_API_TOKEN")
			if e.payload.Token != "" {
				apiToken = e.payload.Token
			}

			graphQLClient, err = api.NewGraphQLClient(e.config.VeritoneAPI, apiToken)
			if err != nil {
				return worker.ErrorReason{
					fmt.Errorf("failed to initialize GraphQL client: %s", err),
					messages.FailureReasonAPINotAllowed,
				}
			}

			tdo, err = setupTDO(ctx, e.payload)
			if err != nil {
				return worker.ErrorReason{
					Err:           err,
					FailureReason: messages.FailureReasonAPIError,
				}
			}
			tdoArg = tdo

			// set up asset storage handler
			assetStore, err = ingest.InitAssetStore(e.config.AssetStorage, graphQLClient, tdo)
			if err != nil {
				return worker.ErrorReason{
					fmt.Errorf("failed to initialize asset storage: %s", err),
					messages.FailureReasonOther,
				}
			}
		}

		// set up chunk cache if outputting chunks
		if e.config.IsChunkProducer() {
			chunkCache, err = streamio.NewS3ChunkCache(e.config.Chunking.Cache)
			if err != nil {
				return worker.ErrorReason{
					fmt.Errorf("failed to initialize chunk cache: %s", err),
					messages.FailureReasonOther,
				}
			}
		}

		if sw != nil {
			sw, err = initStreamIngestor(e.config.IngestorConfig, tdoArg, messagingClient, chunkCache, assetStore, sw)
		} else {
			sw, err = initStreamIngestor(e.config.IngestorConfig, tdoArg, messagingClient, chunkCache, assetStore)
		}

		if err != nil {
			return worker.ErrorReason{
				err,
				messages.FailureReasonInternalError,
			}
		}
	}
	return ingestStream(ctx, e.payload, sr, sw, tdo, e.config)
}
func ingestStream(ctx context.Context, payload *enginePayload, sr streamio.Streamer, sw streamio.StreamWriter, tdo *api.TDO, config *engineConfig) worker.ErrorReason {
	if sw == nil {
		return worker.ErrorReason{
			errors.New("must specify a stream writer"),
			messages.FailureReasonFileWriteError,
		}
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	firstReadTimeout, _ := time.ParseDuration(config.FirstReadTimeout)

	stream, err := streamio.StartStreamReader(sctx, cancel, sr, firstReadTimeout)
	if !config.MediaStartTime.IsZero() {
		stream.StartTime = config.MediaStartTime
	}

	defer func() {
		stream.Close()
	}()

	hb.trackStream(stream)
	hb.trackWrites(sw)

	if err != nil {
		logger.Println(err)
		if err := stream.Err(); err != nil {
			logger.Printf("[2] stream reader err: %s", err)
		}

		return worker.ErrorReason{
			err,
			messages.FailureReasonStreamReaderError,
		}
	}

	if !payload.StreamStartTime.IsZero() {
		// override stream start time with value from payload
		stream.StartTime = payload.StreamStartTime
	}

	streamJSONBytes, _ := json.Marshal(stream)
	logger.Println("Stream Info:", string(streamJSONBytes))
	logger.Println("Mime Type:", stream.MimeType)
	logger.Println("Format:", stream.FfmpegFormat.String())
	logger.Println("Start Offset:", stream.StartOffsetMS)

	var mediaMimeType string
	var segmented bool

	if stream.IsImage() {
		logger.Println("Stream contents contain an image")
		mediaMimeType = stream.MimeType
	} else if stream.IsText() {
		logger.Println("Stream contents contain a text document")
		mediaMimeType = stream.MimeType
	} else if stream.ContainsVideo() {
		mediaMimeType = videoTDOMimeType
		segmented = true
	} else if stream.ContainsAudio() {
		mediaMimeType = audioTDOMimeType
		segmented = true
	} else {
		return worker.ErrorReason{
			errUnsupportedStreamType,
			messages.FailureReasonInvalidData,
		}
	}

	// update TDO info
	if tdo != nil {
		var alterStartStopTimes bool

		// if the stream is at time offset 0, use the start time as the TDO start/stop time
		if stream.StartOffsetMS == 0 && !stream.StartTime.IsZero() {
			logger.Println("Setting TDO start/stop time to:", stream.StartTime.Format(time.RFC3339))
			tdo.StartDateTime = stream.StartTime
			tdo.StopDateTime = stream.StartTime
			alterStartStopTimes = true
		}

		tdo.Status = api.RecordingStatusRecording

		// set veritone-file metadata on TDO
		tdo.Details["veritoneFile"] = api.TDOFileDetails{
			Name:      tdo.Name,
			MimeType:  mediaMimeType,
			Segmented: segmented,
		}

		updatedTDO, err := graphQLClient.UpdateTDO(ctx, *tdo, alterStartStopTimes)
		if err != nil {
			return worker.ErrorReason{
				fmt.Errorf("failed to update TDO: %s", err),
				messages.FailureReasonAPIError,
			}
		}

		tdo.Details = updatedTDO.Details
		tdoJSON, _ := json.Marshal(tdo)
		logger.Println("Updated TDO:", string(tdoJSON))
	}

	errc := make(chan worker.ErrorReason, 4)

	go func() {
		// listen for an error from stream reader
		if err := stream.ErrWait(); err != nil {
			//#TODO check and ignore error in appropriate moment
			// read/write on closed pipe
			if strings.Contains(err.Error(), "read/write on closed pipe") {
				logger.Println("IGNORE stream reder err due to closed pipe..")
				errc <- worker.ErrorReason{}
			} else {
				logger.Println("[1] stream reader err:", err)

				errc <- worker.ErrorReason{
					fmt.Errorf("[1] stream reader err: %s", err),
					messages.FailureReasonStreamReaderError,
				}
			}
		}
	}()

	// transcode stream, if configured
	if payload.TranscodeFormat != "" {
		logger.Printf("transcoding stream from [%s] to [%s]", stream.FfmpegFormat.String(), payload.TranscodeFormat)

		var transodeErrC <-chan error
		stream, transodeErrC = transcodeFn(sctx, stream, payload.TranscodeFormat)

		go func() {
			if err := <-transodeErrC; err != nil {
				logger.Println("transcoder err:", err)
				errc <- worker.ErrorReason{
					fmt.Errorf("transcoder err: %s", err),
					messages.FailureReasonOther,
				}
			}
		}()

		err := stream.GuessMimeType()
		if err != nil && err != streamio.ErrUnknownMimeType {
			logger.Printf("failed to determine mime type of transcoded stream: %s", err)
		}

		probeOutput, err := probeFn(ctx, stream)
		if err != nil {
			return worker.ErrorReason{
				fmt.Errorf("failed to probe transcoded stream: %s", err),
				messages.FailureReasonOther,
			}
		}

		stream.Streams = probeOutput.Streams
		stream.Container = probeOutput.Format
	}
	if stream.MimeType == "" {
		stream.MimeType = mediaMimeType
	}

	var writeTimeout *time.Timer

	globalTimeout, _ := time.ParseDuration(config.GlobalTimeout)
	writeTimeout = time.NewTimer(globalTimeout)
	defer func() {
		writeTimeout.Stop()
	}()

	go func() {
		err := sw.WriteStream(sctx, stream)
		if err != nil {
			errc <- worker.ErrorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		} else {
			errc <- worker.ErrorReason{}
		}
		writeTimeout.Stop()
		fmt.Println("Write stream done")

		cancel() // cancel reader (if it hasn't stopped already)
	}()

	ticker := time.NewTicker(time.Second)
	var lastProgress int64

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				progress := sw.BytesWritten() + stream.BytesRead()
				if tracker, ok := sw.(outputTracker); ok {
					progress += int64(tracker.ObjectsSent())
				}
				if progress != lastProgress {
					lastProgress = progress
					writeTimeout.Reset(globalTimeout)
				}
			case <-writeTimeout.C:
				// We don't want to return an error and fail the task since so far SI only hang after fully
				// ingested the stream (see VTN-29645).  So we log a message and return nil so task will succeed.
				logger.Printf("timeout: stream ingestor has not read/write any new data in %s", config.GlobalTimeout)
				errc <- worker.ErrorReason{}
				return
			}
		}
	}()

	errReason := <-errc

	// update TDO status and any modified metadata
	if tdo != nil {
		tdo.Status = api.RecordingStatusRecorded
		if err != nil {
			tdo.Status = api.RecordingStatusError
		}
		if _, err := graphQLClient.UpdateTDO(ctx, *tdo, false); err != nil {
			logger.Println("failed to update TDO:", err)
			return worker.ErrorReason{
				fmt.Errorf("failed to update TDO: %s", err),
				messages.FailureReasonAPIError,
			}
		}
	}

	return errReason
}

func setupTDO(ctx context.Context, payload *enginePayload) (*api.TDO, error) {
	/*
	if createTDOFlag != nil && *createTDOFlag {
		logger.Println("Creating new TDO...")
		tdo, err := graphQLClient.CreateTDO(ctx, api.NewTDO(uuid.New().String()), payload.TaskID)
		if err != nil {
			return nil, fmt.Errorf("failed to create TDO: %s", err)
		}

		logger.Println("Created TDO:", tdo.ID)
		payload.TDOID = tdo.ID
		return tdo, nil
	}
	*/
	tdo, err := graphQLClient.FetchTDO(ctx, payload.TDOID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch TDO %q: %s", payload.TDOID, err)
	}

	if tdo.Details == nil {
		tdo.Details = make(map[string]interface{})
	}

	return tdo, nil
}
