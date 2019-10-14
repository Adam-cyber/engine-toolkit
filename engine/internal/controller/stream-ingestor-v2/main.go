package siv2

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	messages "github.com/veritone/edge-messages"

	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter/messaging"
	controllerClient "github.com/veritone/realtime/modules/controller/client"

	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio"
	"github.com/veritone/engine-toolkit/engine/internal/controller/worker"
)

const (
	appName     = "SIv2"
	maxAttempts = 5
)

var (
	hb                       *heartbeat
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


func NewStreamIngestor(payloadJSON string,
	engineInstanceId string,
	workItem *controllerClient.EngineInstanceWorkItem,
	workItemStatus *controllerClient.TaskStatusUpdate,
	statusLock *sync.Mutex) (res worker.Worker, err error) {

	method := fmt.Sprintf("[siV2:%s]", engineInstanceId)
	config, payload, err := loadConfigAndPayload(payloadJSON, workItem.EngineId, engineInstanceId)
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
     options: ffmpeg Options e.g  -i {inputfile} xxxx {output}
			For {inputfile} == this comes from the task'input source -- for stream we'll consume in its entirety
                           Even with chunk, it may just be a mini-stream


	   when action == transcode:
            If the inputIOMode == stream:  SIv2 is to consume a stream from the input --> run ffmpeg command with the options --> produce stream as output
            If the inputIOMode == chunk: SIv2 is to consume chunks from the input -> run ffmpeg command with the options --> produce chunks

       When action == chunking:
            SIv2 is to consume a stream --> run ffmpeg command with the options --> produce chunks

       When action == playback
            SIv2 is to consume a stream --> run ffmpeg command with the options --> produce segments to be stored as assets in the TDO for playback


 */

 func (a *internalEngine) Run() (err worker.ErrorReason) {
	method := fmt.Sprintf("[Siv2.Run:%s,%s,%s]", a.workItem.EngineId, a.engineInstanceId, a.workItem.TaskId)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() {
		if a.kafkaClient != nil {
			a.kafkaClient.Close()
		}
	}()
	// Create messaging helper - used for heartbeats and stream writer
	msgr := messaging.NewHelper(a.kafkaClient, a.config.Messaging, messaging.MessageContext{
		JobID:      a.payload.JobID,
		TaskID:     a.payload.TaskID,
		TDOID:      a.payload.TDOID,
		EngineID:   a.config.EngineID,
		InstanceID: a.config.EngineInstanceID,
	})


	// stream writer
	var sw streamio.StreamWriter
	var onCloseFn func(error, int64)

	// get the taskIO for output stream
	// have to find the output
	outputScfsArr, err := scfsio.GetIOForWorkItem(a.workItem, "output")
	if err != nil {
		return errors.Wrapf(err, "%s failed to get OutputIO", method)
	}

	// here goes

	// start engine status heartbeats
	heartbeatInterval, _ := time.ParseDuration(a.config.HeartbeatInterval)
	hb = startHeartbeat(ctx, msgr, heartbeatInterval, sw, a.payload.isOfflineMode())

	defer func() {
		if onCloseFn != nil {
			onCloseFn(err, hb.GetTaskUpTime())
		}
		// send final heartbeat
		status := messages.EngineStatusDone
		if err != nil {
			status = messages.EngineStatusFailed
		}

		log.Println("Sending final heartbeat. Status:", string(status))
		if err := hb.sendHeartbeat(ctx, status, err); err != nil {
			log.Println("Failed to send final heartbeat:", err)
		}
	}()

	return ingestStream(ctx, a.payload, isLive, sr, sw)
}

func ingestStream(ctx context.Context, payload *enginePayload, isLive bool, sr Streamer, sw streamio.StreamWriter) error {
	runTime := time.Now().UTC()
	recordStartTime := payload.RecordStartTime

	if recordStartTime.IsZero() {
		recordStartTime = runTime
	} else if runTime.After(recordStartTime) {
		log.Printf("We've overshot our recording window. Setting start time to %s.", runTime.Format(time.RFC3339))
		recordStartTime = runTime
	}

	var recordDuration time.Duration
	if payload.RecordDuration != "" {
		recordDuration, _ = time.ParseDuration(payload.RecordDuration)
	} else if !payload.RecordEndTime.IsZero() {
		recordDuration = payload.RecordEndTime.Sub(recordStartTime)
	} else if isLive {
		return errorReason{
			errMissingEndTime,
			messages.FailureReasonInvalidData,
		}
	}

	if isLive {
		if recordDuration <= 0 {
			return errorReason{
				errMissedRecordingWindow,
				messages.FailureReasonInvalidData,
			}
		}

		log.Printf("Recording stream %s from %s to %s.",
			payload.URL,
			recordStartTime.Format(time.RFC3339),
			recordStartTime.Add(recordDuration).Format(time.RFC3339))
	}

	// sleep until start time
	if now := time.Now().UTC(); now.Before(recordStartTime) {
		sleepTime := recordStartTime.Sub(now)
		log.Printf("Sleeping for %s", sleepTime.String())
		sleepFunc(sleepTime)
	}

	sctx, cancel := context.WithCancel(ctx)
	stream := sr.Stream(sctx, recordDuration)
	hb.trackReads(stream) // track stream progress in heartbeats

	// if payload specifies an offset in the TDO, pass it along in stream_init message
	if payload.TDOOffsetMS > 0 {
		stream.StartOffsetMS = payload.TDOOffsetMS
	}
	// if payload specifies a start time override, pass it along in stream_init message
	if payload.StartTimeOverride > 0 {
		stream.StartTime = time.Unix(payload.StartTimeOverride, 0).UTC()
	}

	errc := make(chan error, 2)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stream.Close()
		// listen for an error from stream reader
		if err := stream.ErrWait(); err != nil {
			log.Printf("stream reader err: %s", err)
			errc <- errorReason{
				fmt.Errorf("stream reader err: %s", err),
				messages.FailureReasonStreamReaderError,
			}
		}
	}()

	if err := sw.WriteStream(sctx, stream); err != nil {
		log.Printf("stream writer err: %s", err)
		errc <- errorReason{
			fmt.Errorf("stream writer err: %s", err),
			messages.FailureReasonFileWriteError,
		}
	}

	cancel() // cancel stream reader if it has not stopped already
	wg.Wait()
	close(errc)
	return <-errc
}

func urlHasFileExtension(urlStr string, suffixes ...string) bool {
	u, err := url.Parse(urlStr)
	if err != nil {
		log.Printf("Unable to parse url: %s, %s", urlStr, err)
		return false
	}
	for _, v := range suffixes {
		if strings.HasSuffix(u.Path, v) {
			return true
		}
	}
	return false
}

func checkMIMEType(ctx context.Context, urlStr string, httpClient http.Client) (bool, bool, string, error) {
	var stream *streamio.Stream
	var err error

	interval := intialRetryInterval
	httpClient.Timeout = time.Second * 15

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(interval)
			interval = exponentialIncrInterval(interval)
			log.Printf("RETRYING: ATTEMPT %d of %d", attempt, maxAttempts)
		}

		stream, err = tryDetermineMimeType(ctx, &httpClient, urlStr)
		if err == nil {
			break
		}

		log.Printf("failed to determine content type (attempt %d): %s", attempt, err)
	}

	// .IsText() checks the MIME type of the stream, but we also check file ext here just in case MIME isn't set
	isText := stream.IsText() ||
		urlHasFileExtension(urlStr, ".docx", ".doc", ".pdf", ".eml", ".msg", ".txt", ".ppt", ".rtf")

	return stream.IsImage(), isText, stream.MimeType, err
}

func writeEmptyStream(ctx context.Context, sw streamio.StreamWriter) error {
	stream := streamio.NewStream()
	stream.Close()
	return sw.WriteStream(ctx, stream)
}

type retryableError struct {
	error
}

func isErrRetryable(err error) bool {
	_, ok := err.(retryableError)
	return ok
}

func retryableErrorf(format string, args ...interface{}) error {
	return retryableError{fmt.Errorf(format, args...)}
}
