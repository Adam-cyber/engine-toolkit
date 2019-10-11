package adapter

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	messages "github.com/veritone/edge-messages"
	"github.com/veritone/edge-stream-ingestor/streamio"

	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter/api"
	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter/messaging"
	controllerClient "github.com/veritone/realtime/modules/controller/client"

	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio"
)

const (
	appName     = "webstream-adapter"
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

// errorReason is the struct has failure reason and error
type errorReason struct {
	error
	failureReason messages.TaskFailureReason
}

// Streamer is the interface that wraps the Stream method
type Streamer interface {
	Stream(ctx context.Context, dur time.Duration) *streamio.Stream
}

type Adaptor struct {
	engineInstanceId string
	workItem         *controllerClient.EngineInstanceWorkItem
	workItemStatus   *controllerClient.TaskStatusUpdate
	statusLock       *sync.Mutex

	payload *enginePayload
	config  *engineConfig

	apiToken    string
	httpClient  *http.Client
	kafkaClient messaging.Client
}

func (a *Adaptor) Close() {
	if a.kafkaClient != nil {
		a.kafkaClient.Close()
	}
}

func NewAdaptor(payloadJSON string,
	engineInstanceId string,
	workItem *controllerClient.EngineInstanceWorkItem,
	workItemStatus *controllerClient.TaskStatusUpdate,
	statusLock *sync.Mutex) (res *Adaptor, err error) {

	method := fmt.Sprintf("[AdaptorMain:%s]", engineInstanceId)
	config, payload, err := loadConfigAndPayload(payloadJSON, workItem.EngineId, engineInstanceId)
	if err != nil {
		// TODO better error ...
		statusLock.Lock()
		workItemStatus.TaskStatus = "failed"
		workItemStatus.ErrorCount++
		workItemStatus.FailureReason = fmt.Sprintf("Internal")
		workItemStatus.TaskOutput = map[string]interface{}{"error": fmt.Sprintf("Failed to load config and payload, err=%v", err)}
		statusLock.Unlock()
		log.Printf("%s failed to load payload or config, err=%v", method, err)
		return nil, errors.Wrapf(err, "%s failed in loading payload/config", method)
	}

	log.Printf("config=%s", config)
	log.Printf("payload=%s", payload)

	streamio.TextMimeTypes = config.SupportedTextMimeTypes // weird!!

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

	// TODO this is specific to WSA but we may want to keep it here.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: httpStreamerTCPTimeout}).DialContext,
			TLSHandshakeTimeout: httpStreamerTLSTimeout,
		},
	}

	return &Adaptor{
		engineInstanceId: engineInstanceId,
		workItem:         workItem,
		workItemStatus:   workItemStatus,
		statusLock:       statusLock,
		apiToken:         apiToken,
		payload:          payload,
		config:           config,
		httpClient:       httpClient,
		kafkaClient:      kafkaClient,
	}, nil
}

func (a *Adaptor) Run() (err error) {
	method := fmt.Sprintf("[Adaptor.Run:%s,%s,%s]", a.workItem.EngineId, a.engineInstanceId, a.workItem.TaskId)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	// for now, we're going to just assume 1 output???
	scfsStreamWriter := NewSCFSIOWriter(outputScfsArr[0].String(), outputScfsArr[0].ScfsIO, a.payload.ChunkSize)
	if err != nil {
		return errorReason{
			err,
			messages.FailureReasonFileWriteError,
		}
	}

	sw = scfsStreamWriter

	if *stdoutFlag {
		// stdout stream writer
		sw = streamio.NewFileStreamWriter(os.Stdout)
	} else if a.payload.isOfflineMode() && !a.payload.DisableS3 {
		// when payload "CacheToS3Key" is available, webstream adapter is in "offline ingestion" mode
		cacheCfg := streamio.S3CacheConfig{
			Bucket:      a.config.OutputBucketName,
			Region:      a.config.OutputBucketRegion,
			MinioServer: a.config.MinioServer,
		}

		// callback called when the S3 writer has finished writing the file
		onFinishFn := func(url string, startTime int64, endTime int64) {
			msg := messages.EmptyOfflineIngestionRequest()
			msg.ScheduledJobID = a.payload.ScheduledJobID
			msg.SourceID = a.payload.SourceID
			msg.MediaURI = url
			msg.StartTime = startTime
			msg.EndTime = endTime
			msg.SourceTaskSummary.OrgID = strconv.FormatInt(a.payload.OrganizationID, 10)
			msg.SourceTaskSummary.JobID = a.payload.JobID
			msg.SourceTaskSummary.TaskID = a.payload.TaskID
			if hb != nil {
				msg.SourceTaskSummary.BytesRead = hb.BytesRead()
				msg.SourceTaskSummary.BytesWritten = hb.BytesWritten()
			}

			onCloseFn = func(err error, uptime int64) {
				// publish offline ingestion request message when done
				msg.SourceTaskSummary.UpTime = uptime
				if err != nil {
					msg.SourceTaskSummary.Status = messages.EngineStatusFailed
					msg.SourceTaskSummary.ErrorMsg = err.Error()
				} else {
					msg.SourceTaskSummary.Status = messages.EngineStatusDone
				}

				err = msgr.ProduceOffineIngestionRequestMessage(ctx, msg)
				if err == nil {
					log.Printf("Offline ingestion request message sent %+v:", msg)
				} else {
					log.Printf("Failed to send offline ingestion request message: %s", err)
				}
			}
		}

		log.Println("Enabling S3 Stream Writer")
		s3StreamWriter, err := NewS3StreamWriter(a.payload.CacheToS3Key, cacheCfg, onFinishFn)
		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		}

		sw, err = NewTeeStreamWriter(s3StreamWriter, scfsStreamWriter)
		if err != nil {
			return errorReason{
				err,
				messages.FailureReasonFileWriteError,
			}
		}

	}

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

	// determine source URL
	var isLive bool
	url := a.payload.URL
	if url == "" {
		var sourceDetails api.SourceDetails
		if a.payload.SourceDetails != nil {
			sourceDetails = *a.payload.SourceDetails
			isLive = true // assume offline ingestion mode is used for live streams only
		} else {
			// fetch the URL from the Source configuration
			coreAPIClient, err := api.NewCoreAPIClient(a.config.VeritoneAPI, a.apiToken)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to initialize core API client: %s", err),
					messages.FailureReasonAPIError,
				}
			}

			source, err := coreAPIClient.FetchSource(ctx, a.payload.SourceID)
			if err != nil {
				return errorReason{
					fmt.Errorf("failed to fetch source %q: %s", a.payload.SourceID, err),
					messages.FailureReasonURLNotFound,
				}
			}

			sourceDetails = source.Details
			isLive = source.SourceType.IsLive
		}

		if sourceDetails.URL != "" {
			url = sourceDetails.URL
		} else if sourceDetails.RadioStreamURL != "" {
			url = sourceDetails.RadioStreamURL
		} else {
			return errorReason{
				fmt.Errorf("source %q does not have a URL field set", a.payload.SourceID),
				messages.FailureReasonAPINotFound,
			}
		}
	}

	isLive = isLive || a.payload.isTimeBased()
	log.Printf("Stream URL: %s Live: %v", url, isLive)

	// set up the stream reader
	var sr Streamer

	switch true {
	case rtspURLRegexp.MatchString(url):
		sr = newRTSPReader(url)

	case httpURLRegexp.MatchString(url):
		// hack: provide JWT as Authorization header for media-streamer URLs
		if strings.Contains(url, "veritone") && strings.Contains(url, "/media-streamer/stream") {
			a.httpClient, err = api.NewAuthenticatedHTTPClient(a.config.VeritoneAPI, a.apiToken)
			if err != nil {
				return fmt.Errorf("http client err: %s", err)
			}
		}

		isImage, isText, mimeType, err := checkMIMEType(ctx, url, *a.httpClient)
		if err != nil && err != streamio.ErrUnknownMimeType {
			return errorReason{
				err,
				messages.FailureReasonInvalidData,
			}
		}
		if mimeType != "" {
			log.Println("Detected MIME Type:", mimeType)
		}
		if isImage || isText {
			log.Println("Using HTTP Reader.")
			sr = newHTTPStreamer(a.httpClient, url)
			break
		}
		if strings.HasPrefix(mimeType, mpjpegMimeType) ||
			urlHasFileExtension(url, ".mjpeg", ".mjpg", ".cgi") {
			log.Println("Using MJPEG Reader.")
			sr = newMJPEGReader(a.config.FFMPEG, url)
			break
		}
		if strings.HasPrefix(mimeType, mpegDASHMimeType) {
			// hack: streamlink has a bug where it doesn't return any valid streams when the DASH manifest
			// only has a single audio stream and no video. Clone the manifest on disk and insert a dummy
			// audio stream to work around this.
			url, err = patchMPD(ctx, url, a.httpClient)
			if err != nil {
				log.Println("failed to patch DASH manifest for streamlink:", err)
			}
		}
		// if not an image or supported text document, fall through to default (streamlink or ffmpeg)
		fallthrough

	default:
		if canStreamLinkHandleURL(ctx, url) {
			log.Println("Using StreamLink Reader.")
			sr = newStreamlinkReader(url)
		} else if isLive {
			log.Println("Using FFMPEG Reader.")
			sr = newFFMPEGReader(a.config.FFMPEG, url)
		} else {
			log.Println("Using HTTP Reader.")
			sr = newHTTPStreamer(a.httpClient, url)
		}
	}

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
