package SIv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pborman/uuid"
	messages "github.com/veritone/edge-messages"
	"github.com/veritone/edge-stream-ingestor/api"
	"github.com/veritone/edge-stream-ingestor/ingest"
	"github.com/veritone/edge-stream-ingestor/messaging"
	"github.com/veritone/edge-stream-ingestor/streamio"

	"github.com/veritone/realtime/modules/scfs"
	"github.com/veritone/realtime/modules/utilities"
)

const (
	appName          = "edge-stream-ingestor"
	videoTDOMimeType = "video/mp4"
	audioTDOMimeType = "audio/mp4"
)

// errorReason is the struct has failure reason and error
type errorReason struct {
	error
	failureReason messages.TaskFailureReason
}

var (
	// BuildCommitHash is set at build time (see Makefile)
	BuildCommitHash string
	// BuildTime is set at build time (see Makefile)
	BuildTime string
)

var (
	logger        = log.New(os.Stderr, "[ main ] ", log.LstdFlags)
	transcodeFn   = ingest.TranscodeStream
	probeFn       = streamio.ProbeStream
	hb            *heartbeat
	graphQLClient api.CoreAPIClient
)

/**
assumption:

given a payload with taskIO etc
and desired output:

chunk

 */
func StreamIngestorMain() {
	// Log build info
	logger.Printf("Build time: %s, Build commit hash: %s", BuildTime, BuildCommitHash)
	logger.Println("CPUs:", runtime.NumCPU())
	// should also dump out the env variables
	logger.Println("ENVIRONMENT VARIABLES")
	for _, v := range os.Environ() {
		logger.Println(v)
	}

	config, payload, err := loadConfigAndPayload()
	if err != nil {
		log.Fatal("error initializing payload and config: ", err)
	}

	// apply payload overrides to ingestor config
	config.applyOverrides(*payload)

	logger.Printf("config=%s", config)
	logger.Printf("payload=%s", payload)

	streamio.TextMimeTypes = config.SupportedTextMimeTypes

	if err := initAndRun(config, payload); err != nil {
		logger.Printf("TASK FAILED [%s]: %s", payload.TaskID, err)
		logger.Println("goodbye")
		os.Exit(1)
	}

	logger.Println("done")
}

func initAndRun(config *engineConfig, payload *enginePayload) (err error) {
	// Create messaging client
	kafkaClient, err := messaging.NewKafkaClient(config.Messaging.Kafka)
	if err != nil {
		return err
	}
	defer kafkaClient.Close()

	messagingClient := messaging.NewHelper(kafkaClient, config.Messaging, messaging.MessageContext{
		AppName:    appName,
		JobID:      payload.JobID,
		TaskID:     payload.TaskID,
		TDOID:      payload.TDOID,
		EngineID:   config.EngineID,
		InstanceID: config.EngineInstanceID,
	})

	// root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start engine status heartbeats
	heartbeatInterval, _ := time.ParseDuration(config.HeartbeatInterval)
	hb = startHeartbeat(ctx, messagingClient, heartbeatInterval)

	defer func() {
		// send final heartbeat
		status := messages.EngineStatusDone
		if err != nil {
			status = messages.EngineStatusFailed
		}

		err := hb.sendHeartbeat(ctx, status, err, lastStreamInfo)
		if err != nil {
			logger.Println("Failed to send final heartbeat:", err)
		}
	}()

	var (
		sr  streamio.Streamer
		sw  streamio.StreamWriter
		tdo *api.TDO
	)

	// trying to read from scfs
	taskFolderName, err := searchInputTaskFolder(&payload.inputPayload)
	if err == nil && taskFolderName != "" {
		logger.Printf("Using the %s task folder", taskFolderName)

		taskOutputFolderName := filepath.Join(taskFolderName, "output")
		statusFileName := filepath.Join(taskFolderName, "status.json")

		sr, err = scfs.NewScfsStreamReader(taskOutputFolderName, statusFileName)

		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonStreamReaderError,
			}
		}

	} else { // in case we were not able to read from scfs - go back to the default implementation
		logger.Printf("Cannot find the task folder: %s. Fallback to the default implementation.\n", err.Error())

		// set up the stream reader
		if *stdinFlag {
			logger.Println("reading stream from stdin")
			sr = streamio.NewFileReader(os.Stdin, *stdinMimeTypeFlag, *stdinFormatFlag)

		} else if config.InputTopicName != "" { // using kafka

			logger.Printf("reading stream from topic: %s:%d", config.InputTopicName, config.InputTopicPartition)
			cfg := streamio.MessageReaderConfig{
				Topic:     config.InputTopicName,
				Partition: config.InputTopicPartition,
				KeyPrefix: config.InputTopicKeyPrefix,
			}
			sr, err = streamio.NewMessageStreamReader(cfg, kafkaClient)

			if err != nil {
				return errorReason{
					err,
					messages.FailureReasonStreamReaderError,
				}
			}
		} else if payload.URL != "" { // offline ingestion
			logger.Println("reading stream from URL:", payload.URL)
			sr = streamio.NewHTTPStreamer(streamio.DefaultHTTPClient, payload.URL)
		} else {
			return errorReason{
				errors.New("stream input topic or URL is required"),
				messages.FailureReasonInvalidData,
			}
		}
	}

	// set up stream writer, if configured
	if *stdoutFlag {
		// stdout stream writer
		sw = streamio.NewFileStreamWriter(os.Stdout)
	} else if dest := *outputFlag; dest != "" {
		// file stream writer
		file, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		}

		sw = streamio.NewFileStreamWriter(file)
	} else if config.OutputTopicName != "" {
		// kafka message stream writer
		sw = streamio.NewMessageStreamWriter(messagingClient, streamio.MessageStreamWriterConfig{
			Topic:     config.OutputTopicName,
			Partition: config.OutputTopicPartition,
			KeyPrefix: config.OutputTopicKeyPrefix,
		})
	}

	if config.IsAssetProducer() || config.IsChunkProducer() {
		var chunkCache streamio.Cacher
		var assetStore *ingest.AssetStore

		tdoArg := &api.TDO{
			ID:      payload.TDOID,
			Details: make(map[string]interface{}),
		}

		// set up asset store if generating assets
		if config.IsAssetProducer() {
			// payload token overrides token from environment variable
			apiToken := os.Getenv("VERITONE_API_TOKEN")
			if payload.Token != "" {
				apiToken = payload.Token
			}

			graphQLClient, err = api.NewGraphQLClient(config.VeritoneAPI, apiToken)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to initialize GraphQL client: %s", err),
					messages.FailureReasonAPINotAllowed,
				}
			}

			tdo, err = setupTDO(ctx, payload)
			if err != nil {
				return err
			}
			tdoArg = tdo

			// set up asset storage handler
			assetStore, err = ingest.InitAssetStore(config.AssetStorage, graphQLClient, tdo)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to initialize asset storage: %s", err),
					messages.FailureReasonOther,
				}
			}
		}

		// set up chunk cache if outputting chunks
		if config.IsChunkProducer() {
			chunkCache, err = streamio.NewS3ChunkCache(config.Chunking.Cache)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to initialize chunk cache: %s", err),
					messages.FailureReasonOther,
				}
			}
		}

		if sw != nil {
			sw, err = initStreamIngestor(config.IngestorConfig, tdoArg, messagingClient, chunkCache, assetStore, sw)
		} else {
			sw, err = initStreamIngestor(config.IngestorConfig, tdoArg, messagingClient, chunkCache, assetStore)
		}

		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonInternalError,
			}
		}
	}

	if sw == nil {
		logger.Printf("taskId:%s, no output is needed -- reProcessJob?", payload.TaskID)

		//There is a race condition in scheduler. This just temporily avoid it.
		//The real fix is pending for discussion: https://github.com/veritone/edge-scheduler/pull/195
		time.Sleep(time.Second * 10)

		// reset since we don't need it
		err = nil
	} else {
		// must capture err here so that the deferred func can use it
		err = ingestStream(ctx, payload, sr, sw, tdo, config)
	}

	return err
}

func setupTDO(ctx context.Context, payload *enginePayload) (*api.TDO, error) {
	if createTDOFlag != nil && *createTDOFlag {
		logger.Println("Creating new TDO...")
		tdo, err := graphQLClient.CreateTDO(ctx, api.NewTDO(uuid.New()), payload.TaskID)
		if err != nil {
			return nil, fmt.Errorf("failed to create TDO: %s", err)
		}

		logger.Println("Created TDO:", tdo.ID)
		payload.TDOID = tdo.ID
		return tdo, nil
	}

	tdo, err := graphQLClient.FetchTDO(ctx, payload.TDOID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch TDO %q: %s", payload.TDOID, err)
	}

	if tdo.Details == nil {
		tdo.Details = make(map[string]interface{})
	}

	return tdo, nil
}

func ingestStream(ctx context.Context, payload *enginePayload, sr streamio.Streamer, sw streamio.StreamWriter, tdo *api.TDO, config *engineConfig) error {
	if sw == nil {
		return errorReason{
			errors.New("must specify a stream writer"),
			messages.FailureReasonFileWriteError,
		}
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := streamio.StartStreamReader(sctx, sr)
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

		return errorReason{
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
		return errorReason{
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
			return errorReason{
				fmt.Errorf("failed to update TDO: %s", err),
				messages.FailureReasonAPIError,
			}
		}

		tdo.Details = updatedTDO.Details
		tdoJSON, _ := json.Marshal(tdo)
		logger.Println("Updated TDO:", string(tdoJSON))
	}

	errc := make(chan error, 4)

	go func() {
		// listen for an error from stream reader
		if err := stream.ErrWait(); err != nil {
			//#TODO check and ignore error in appropriate moment
			// read/write on closed pipe
			if strings.Contains(err.Error(), "read/write on closed pipe") {
				logger.Println("IGNORE stream reder err due to closed pipe..")
				errc <- nil
			} else {
				logger.Println("[1] stream reader err:", err)

				errc <- errorReason{
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
				errc <- errorReason{
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
			return errorReason{
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
			errc <- errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		} else {
			errc <- nil
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
				errc <- nil
				return
			}
		}
	}()

	err = <-errc

	// update TDO status and any modified metadata
	if tdo != nil {
		tdo.Status = api.RecordingStatusRecorded
		if err != nil {
			tdo.Status = api.RecordingStatusError
		}
		if _, err := graphQLClient.UpdateTDO(ctx, *tdo, false); err != nil {
			logger.Println("failed to update TDO:", err)
			return errorReason{
				fmt.Errorf("failed to update TDO: %s", err),
				messages.FailureReasonAPIError,
			}
		}
	}

	return err
}

func searchInputTaskFolder(inputPayload *inputPayload) (string, error) {
	logger.Println("Searching for the input task folder")
	var retryCount int
	var taskFolderName string
	var err error
	totalRetryCount := 60

	if inputPayload.OrgID == "" || inputPayload.JobID == "" || inputPayload.TaskID == "" {
		return "", errors.New("Empty or invalid input params. Skipping the retry logic.")
	}

	logger.Printf("Total RetryCount: %d\n", totalRetryCount)
	for retryCount < totalRetryCount {
		taskFolderName, err = getAdapterOutputFolder(inputPayload)

		if err == nil {
			break
		} else {
			retryCount++
			fmt.Println(err)
			fmt.Printf("Cannot find the task folder. RetryCount: %d. Sleeping for 1 second\n", retryCount)
			time.Sleep(1 * time.Second)
		}
	}

	return taskFolderName, err
}

func getAdapterOutputFolder(inputPayload *inputPayload) (string, error) { // TODO: merge wih logic from scfs.InitScfsStreamWriter()

	if inputPayload.OrgID == "" || inputPayload.JobID == "" || inputPayload.TaskID == "" {
		return "", errors.New("Empty or invalid input params")
	}

	cacheOutputFolder := scfs.GetCacheOutputFolder(inputPayload.OrgID)

	orgID := "00"

	if len(inputPayload.OrgID) == 1 {
		orgID = "0" + inputPayload.OrgID
	} else if len(inputPayload.OrgID) > 1 {
		orgID = inputPayload.OrgID
	}

	log.Printf("OrganizationID: " + orgID)

	taskFolder := filepath.Join(
		cacheOutputFolder,
		strings.ToLower(orgID[len(orgID)-2:]),
		orgID,
		"job",
		strings.ToLower(inputPayload.JobID[len(inputPayload.JobID)-2:]),
		inputPayload.JobID,
		"task",
		inputPayload.TaskID,
	)

	if !utilities.FolderExists(taskFolder) {
		return "", fmt.Errorf("The task folder %s does not exist", taskFolder)
	}

	return taskFolder, nil

}
