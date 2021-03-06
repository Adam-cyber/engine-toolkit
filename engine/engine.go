package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/veritone/realtime/modules/engines/scfsio"
	"github.com/veritone/realtime/modules/engines/toolkit/controller"
	"github.com/veritone/realtime/modules/engines/toolkit/processing"
	"github.com/veritone/realtime/modules/engines/toolkit/selfdriving"
	"github.com/veritone/realtime/modules/engines/toolkit/vericlient"
	rtLogger "github.com/veritone/realtime/modules/logger"
)

const (
	dirInput   = "/files/in"
	dirMoveTo  = "/files/out/completed"
	dirErr     = "/files/out/errors"
	dirResults = "/files/out/results"
)

// Engine consumes messages and calls webhooks to
// fulfil the requests.
type Engine struct {
	producer      processing.Producer
	eventProducer processing.Producer
	consumer      processing.Consumer

	// processingSemaphore is a buffered channel that controls how
	// many concurrent processing tasks will be performed.
	processingSemaphore chan struct{}

	testMode bool

	// client is the client to use to make webhook requests.
	webhookClient *http.Client
	// graphQLHTTPClient is the client used to access GraphQL.
	graphQLHTTPClient *http.Client

	logDebug func(args ...interface{})

	// Config holds the Engine configuration.
	Config Config

	// processing time
	processingDurationLock sync.RWMutex
	processingDuration     time.Duration

	// Controller specific
	controller *controller.ControllerUniverse
	logger     rtLogger.Logger
}

// NewEngine makes a new Engine with the specified Consumer and Producer.
// Logging:  /cache/logs/engineInstaces/{engineInstanceId}.log
//
func NewEngine() *Engine {
	// generate engineInstanceId and use that for logging
	engineInstanceId := os.Getenv("ENGINE_INSTANCE_ID")
	if engineInstanceId == "" {
		engineInstanceId = scfsio.GenerateUuid()
	}
	logFileName, logWriter, logger := scfsio.GetEngineInstanceLogFile(engineInstanceId)

	return &Engine{
		logDebug: func(args ...interface{}) {
			logger.Debug(args...)
		},
		Config:            NewConfig(engineInstanceId, logFileName, logWriter, logger),
		webhookClient:     &http.Client{ /* no timeout */ },
		graphQLHTTPClient: &http.Client{Timeout: 30 * time.Minute},
	}
}

// isTrainingTask gets whether the task is a training task or not.
func isTrainingTask() (bool, error) {
	payload, err := EnvPayload()
	if err == ErrNoPayload {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return payload.Mode == "library-train", nil
}

func (e *Engine) Terminate() {
	// close up stuff
	if e.Config.ControllerConfig.LogWriter != nil {
		e.Config.ControllerConfig.LogWriter.Close()
		// also direct the logger to just os
		if e.Config.ControllerConfig.Logger != nil {
			if e.Config.ControllerConfig.Logger.GetLogrus() != nil {
				e.Config.ControllerConfig.Logger.GetLogrus().Out = os.Stdout
			}
		}
	}
}

// Run runs the Engine.
// Context errors may be returned.
// TODO For controller route, we need to deal with batch, library engine training from within the loop
//
//
func (e *Engine) Run(ctx context.Context) error {
	if e.controller != nil {
		e.logDebug("Running in Controller mode")
		return e.runViaController(ctx)
	}

	isTraining, err := isTrainingTask()
	if err != nil {
		return errors.Wrap(err, "isTrainingTask")
	}
	if e.testMode {
		go e.runTestConsole(ctx)
		e.logDebug("running subprocess for testing...")
		return e.runSubprocessOnly(ctx)
	}
	// for V3  -- even with training, we must report the status for the task!
	if isTraining {
		e.logDebug("running subprocess for training...")
		return e.runSubprocessOnly(ctx)
	}
	semaphoreSize := 1
	if e.Config.Processing.Concurrency > 0 {
		semaphoreSize = e.Config.Processing.Concurrency
	}
	e.processingSemaphore = make(chan struct{}, semaphoreSize)
	if e.Config.SelfDriving.SelfDrivingMode {
		e.logDebug("running inference in file system mode...")
		return e.runInferenceFSMode(ctx)
	}

	e.logDebug("running inference...")
	return e.runInference(ctx)
}

// runSubprocessOnly starts the subprocess and doesn't do anything else.
// This is used for training tasks.
func (e *Engine) runSubprocessOnly(ctx context.Context) error {
	if len(e.Config.Subprocess.Arguments) < 1 {
		return errors.New("not enough arguments to run subprocess")
	}
	cmd := exec.CommandContext(ctx, e.Config.Subprocess.Arguments[0], e.Config.Subprocess.Arguments[1:]...)
	cmd.Stdout = e.Config.Stdout
	cmd.Stderr = e.Config.Stderr
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, e.Config.Subprocess.Arguments[0])
	}
	return nil
}

// runInferenceFSMode starts the subprocess and routes work to webhooks
// drawing work from the input folder, sending output to the output folder.
func (e *Engine) runInferenceFSMode(ctx context.Context) error {
	go func() {
		if err := e.runSubprocessOnly(ctx); err != nil {
			e.logDebug("runSubprocessOnly: error:", err)
		}
	}()
	readyCtx, cancel := context.WithTimeout(ctx, e.Config.Subprocess.ReadyTimeout)
	defer cancel()
	e.logDebug("waiting for ready... will expire after", e.Config.Subprocess.ReadyTimeout)
	if err := e.ready(readyCtx); err != nil {
		return err
	}
	logger := log.New(os.Stdout, "", log.LstdFlags)
	sel := &selfdriving.RandomSelector{
		Rand:                    rand.New(rand.NewSource(time.Now().UnixNano())),
		Logger:                  logger,
		PollInterval:            e.Config.SelfDriving.PollInterval,
		MinimumModifiedDuration: e.Config.SelfDriving.MinimumModifiedDuration,
		InputDir:                dirInput,
		InputPattern:            e.Config.SelfDriving.InputPattern,
		WaitForReadyFiles:       e.Config.SelfDriving.WaitForReadyFiles,
	}
	processor := &selfdriving.Processor{
		Logger:           logger,
		Selector:         sel,
		OutputDirPattern: e.Config.SelfDriving.OutputDirPattern,
		MoveToDir:        dirMoveTo,
		ErrDir:           dirErr,
		ResultsDir:       dirResults,
		Process:          e.processSelfDrivingFile,
	}
	if err := processor.Run(ctx); err != nil {
		return errors.Wrap(err, "processor")
	}
	return nil
}
func (e *Engine) getSelfDrivingPayloadFile(inputDir string) ([]byte, error) {
	payloadFilepath := filepath.Join(inputDir, "payload.json")
	_, err := os.Stat(payloadFilepath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadFile(payloadFilepath)
	if err != nil {
		return nil, errors.Wrap(err, "read self-driving payload.json")
	}
	e.logDebug("using payload.json: %s: %s", payloadFilepath, string(b))
	return b, nil
}
func (e *Engine) processSelfDrivingFile(outputDir string, file selfdriving.File) error {
	e.logDebug("processing file:", file)
	payloadJSON, err := e.getSelfDrivingPayloadFile(dirInput)
	if err != nil {
		return err
	}
	req, err := processing.NewRequestFromFile(e.Config.Webhooks.Process.URL, file, payloadJSON)
	if err != nil {
		return errors.Wrap(err, "new request")
	}
	resp, err := e.webhookClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		e.logDebug("ignoring chunk after StatusNoContent:", file.Path)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			return errors.Wrap(err, "read body")
		}
		return errors.Errorf("%d: %s", resp.StatusCode, strings.TrimSpace(buf.String()))
	}
	if resp.ContentLength == 0 {
		e.logDebug("no data to output for file:", file.Path)
		return nil
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		e.logDebug("content type parsing failed, assuming json:", err)
	}
	// file response
	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "reading multipart response")
			}
			defer p.Close()
			outputFile := filepath.Join(outputDir, p.FileName())
			err = writeOutputFile(outputFile, p)
			if err != nil {
				return errors.Wrap(err, "writing output file")
			}
			return nil
		}
		return nil
	}

	// json response
	outputFile := filepath.Join(outputDir, filepath.Base(file.Path)+".json")
	err = writeOutputFile(outputFile, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func writeOutputFile(outputFile string, r io.Reader) error {
	f, err := os.Create(outputFile)
	if err != nil {
		return errors.Wrap(err, "create")
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return errors.Wrap(err, "read body")
	}
	// make the output file ready
	out := selfdriving.File{Path: outputFile}
	err = out.Ready()
	if err != nil {
		return errors.Wrap(err, "write ready file")
	}
	return nil
}

// runInference starts the subprocess and routes work to webhooks.
// This is used to process files.
func (e *Engine) runInference(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cmd *exec.Cmd
	if len(e.Config.Subprocess.Arguments) > 0 {
		cmd = exec.CommandContext(ctx, e.Config.Subprocess.Arguments[0], e.Config.Subprocess.Arguments[1:]...)
		cmd.Stdout = e.Config.Stdout
		cmd.Stderr = e.Config.Stderr
		if err := cmd.Start(); err != nil {
			return errors.Wrap(err, e.Config.Subprocess.Arguments[0])
		}
		readyCtx, cancel := context.WithTimeout(ctx, e.Config.Subprocess.ReadyTimeout)
		defer cancel()
		e.logDebug("waiting for ready... will expire after", e.Config.Subprocess.ReadyTimeout)
		if err := e.ready(readyCtx); err != nil {
			return err
		}
	}
	e.logDebug(fmt.Sprintf("processing %d task(s) concurrently", e.Config.Processing.Concurrency))
	e.logDebug("waiting for messages...")
	e.sendEvent(event{
		Key:  e.Config.Engine.ID,
		Type: eventStart,
	})
	go e.sendPeriodicEvents(ctx)
	go func() {
		var wg sync.WaitGroup
		defer func() {
			e.logDebug("waiting for jobs to finish...")
			wg.Wait()
			e.logDebug("shutting down...")
			e.sendEvent(event{
				Key:  e.Config.Engine.ID,
				Type: eventStop,
			})
			cancel()
		}()
		for {
			select {
			case msg, ok := <-e.consumer.Messages():
				if !ok {
					// consumer has closed down
					return
				}
				e.consumer.MarkOffset(msg, "")
				select {
				case <-ctx.Done():
					return
				case e.processingSemaphore <- struct{}{}:
					// try to put something into the semaphore
					// this will block if the channel is full
					// causing processing to pause - it will unblock
					// when we release the semaphore.
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := e.processMessage(ctx, msg); err != nil {
						e.logDebug(fmt.Sprintf("processing error: %v", err))
					}
					// release the semaphore
					<-e.processingSemaphore
				}()
			case <-time.After(e.Config.Engine.EndIfIdleDuration):
				e.logDebug(fmt.Sprintf("idle for %s", e.Config.Engine.EndIfIdleDuration))
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	if cmd != nil {
		// wait for the command
		if err := cmd.Wait(); err != nil {
			if err := ctx.Err(); err != nil {
				// if the context has an error, we'll assume this command
				// errored because we terminated it (via context).
				return ctx.Err()
			}
			// otherwise, the subprocess has crashed
			return errors.Wrap(err, e.Config.Subprocess.Arguments[0])
		}
		return nil
	}
	<-ctx.Done()
	return nil
}

func (e *Engine) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	start := time.Now()
	defer func() {
		now := time.Now()
		e.addProcessingTime(now.Sub(start))
	}()
	var typeCheck struct {
		Type processing.MessageType
	}
	if err := json.Unmarshal(msg.Value, &typeCheck); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	switch typeCheck.Type {
	case processing.MessageTypeMediaChunk:
		if err := e.processMessageMediaChunk(ctx, msg); err != nil {
			return errors.Wrap(err, "process media chunk")
		}
	default:
		e.logDebug(fmt.Sprintf("ignoring message of type %q: %+v", typeCheck.Type, msg))
	}
	return nil
}

// processMessageMediaChunk processes a single media chunk as described by the sarama.ConsumerMessage.
func (e *Engine) processMessageMediaChunk(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var mediaChunk processing.MediaChunkMessage
	if err := json.Unmarshal(msg.Value, &mediaChunk); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	e.sendEvent(event{
		Key:     mediaChunk.ChunkUUID,
		Type:    eventConsumed,
		JobID:   mediaChunk.JobID,
		TaskID:  mediaChunk.TaskID,
		ChunkID: mediaChunk.ChunkUUID,
	})
	finalUpdateMessage := processing.ChunkResult{
		Type:      processing.MessageTypeChunkResult,
		TaskID:    mediaChunk.TaskID,
		ChunkUUID: mediaChunk.ChunkUUID,
		Status:    processing.ChunkStatusSuccess, // optimistic
	}
	defer func() {
		// send the final (ChunkResult) message
		finalUpdateMessage.TimestampUTC = time.Now().Unix()
		_, _, err := e.producer.SendMessage(&sarama.ProducerMessage{
			Topic: e.Config.Kafka.ChunkTopic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: processing.NewJSONEncoder(finalUpdateMessage),
		})
		if err != nil {
			e.logDebug("WARN", "failed to send final chunk update:", err)
		}
		e.sendEvent(event{
			Key:     mediaChunk.ChunkUUID,
			Type:    eventProduced,
			JobID:   mediaChunk.JobID,
			TaskID:  mediaChunk.TaskID,
			ChunkID: mediaChunk.ChunkUUID,
		})
	}()
	ignoreChunk := false
	retry := processing.NewDoubleTimeBackoff(
		e.Config.Webhooks.Backoff.InitialBackoffDuration,
		e.Config.Webhooks.Backoff.MaxBackoffDuration,
		e.Config.Webhooks.Backoff.MaxRetries,
	)
	var content string
	err := retry.Do(func() error {
		req, err := processing.NewRequestFromMediaChunk(e.webhookClient, e.Config.Webhooks.Process.URL,
			mediaChunk, e.Config.Processing.DisableChunkDownload, "" ,"", "", 0)
		if err != nil {
			return errors.Wrap(err, "new request")
		}
		req = req.WithContext(ctx)
		resp, err := e.webhookClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			ignoreChunk = true
			return nil
		}
		if resp.StatusCode != http.StatusOK {
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, resp.Body); err != nil {
				return errors.Wrap(err, "read body")
			}
			return errors.Errorf("%d: %s", resp.StatusCode, strings.TrimSpace(buf.String()))
		}
		if resp.ContentLength == 0 {
			ignoreChunk = true
			return nil
		}
		mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil {
			e.logDebug("content type parsing failed, assuming json:", err)
		}
		if strings.HasPrefix(mediaType, "multipart/") {
			// files output
			payload, err := mediaChunk.UnmarshalPayload()
			if err != nil {
				return errors.Wrap(err, "unmarshal payload")
			}
			type mediaItem struct {
				AssetID     string `json:"assetId"`
				ContentType string `json:"contentType"`
			}
			var outputJSON struct {
				Media []mediaItem `json:"media"`
			}
			mr := multipart.NewReader(resp.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrap(err, "reading multipart response")
				}
				assetCreate := processing.AssetCreate{
                                        AssetType:      "media",
					ContainerTDOID: mediaChunk.TDOID,
					ContentType:    p.Header.Get("Content-Type"),
					Name:           p.FileName(),
					Body:           p,
				}
				client := vericlient.NewClient(e.graphQLHTTPClient, payload.Token, payload.VeritoneAPIBaseURL+"/v3/graphql")
				createdAsset, err := assetCreate.Do(ctx, client)
				if err != nil {
					return errors.Wrapf(err, "create asset for %s", p.FileName())
				}
				outputJSON.Media = append(outputJSON.Media, mediaItem{
					AssetID:     createdAsset.ID,
					ContentType: createdAsset.ContentType,
				})
				return nil
			}
			jsonBytes, err := json.Marshal(outputJSON)
			if err != nil {
				return errors.Wrap(err, "encode output JSON")
			}
			content = string(jsonBytes)
		} else {
			// JSON output
			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return errors.Wrap(err, "read response body")
			}
			content = string(bodyBytes)
		}
		return nil
	})
	if err != nil {
		// send error message
		finalUpdateMessage.Status = processing.ChunkStatusError
		finalUpdateMessage.ErrorMsg = err.Error()
		finalUpdateMessage.FailureReason = "internal_error"
		finalUpdateMessage.FailureMsg = finalUpdateMessage.ErrorMsg
		return err
	}
	if ignoreChunk {
		finalUpdateMessage.Status = processing.ChunkStatusIgnored
		return nil
	}
	// send output message
	outputMessage := processing.MediaChunkMessage{
		Type:          processing.MessageTypeEngineOutput,
		TaskID:        mediaChunk.TaskID,
		JobID:         mediaChunk.JobID,
		ChunkUUID:     mediaChunk.ChunkUUID,
		StartOffsetMS: mediaChunk.StartOffsetMS,
		EndOffsetMS:   mediaChunk.EndOffsetMS,
		TimestampUTC:  time.Now().Unix(),
		Content:       content,
	}
	tmp, _ := json.Marshal(outputMessage)
	e.logDebug("outputMessage will be sent to kafka: ", string(tmp))
	finalUpdateMessage.TimestampUTC = time.Now().Unix()
	finalUpdateMessage.EngineOutput = &outputMessage
	return nil
}

// ready returns a channel that is closed when the engine is
// ready.
// The channel may receive an error if something goes wrong while waiting
// for the engine to become ready.
// QD: Didn't seem to reflect the above comments.  It just polling the webhook for ready?
func (e *Engine) ready(ctx context.Context) error {
	start := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().Sub(start) >= e.Config.Webhooks.Ready.MaximumPollDuration {
			e.logDebug("ready: exceeded", e.Config.Webhooks.Ready.MaximumPollDuration)
			return errReadyTimeout
		}
		resp, err := http.Get(e.Config.Webhooks.Ready.URL)
		if err != nil {
			e.logDebug("ready: err:", err)
			time.Sleep(e.Config.Webhooks.Ready.PollDuration)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			e.logDebug("ready: status:", resp.Status)
			time.Sleep(e.Config.Webhooks.Ready.PollDuration)
			continue
		}
		e.logDebug("ready: yes")
		return nil
	}
}

// ProcessingDuration gets the current processing duration.
func (e *Engine) ProcessingDuration() time.Duration {
	e.processingDurationLock.RLock()
	defer e.processingDurationLock.RUnlock()
	return e.processingDuration
}

// addProcessingTime adds the d to the current processing duration.
func (e *Engine) addProcessingTime(d time.Duration) {
	e.processingDurationLock.Lock()
	defer e.processingDurationLock.Unlock()
	e.processingDuration += d
}

// errReadyTimeout is sent down the Ready channel if the
// Webhooks.Ready.MaximumPollDuration is exceeded.
var errReadyTimeout = errors.New("ready: maximum duration exceeded")
