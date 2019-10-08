package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/veritone/engine-toolkit/engine/internal/selfdriving"
)

// Engine consumes messages and calls webhooks to
// fulfil the requests.
type Engine struct {
	producer      Producer
	eventProducer Producer
	consumer      Consumer

	// processingSemaphore is a buffered channel that controls how
	// many concurrent processing tasks will be performed.
	processingSemaphore chan struct{}

	testMode bool

	client   *http.Client
	logDebug func(args ...interface{})

	// Config holds the Engine configuration.
	Config Config

	// processing time
	processingDurationLock sync.RWMutex
	processingDuration     time.Duration
}

// NewEngine makes a new Engine with the specified Consumer and Producer.
func NewEngine() *Engine {
	return &Engine{
		logDebug: func(args ...interface{}) {
			log.Println(args...)
		},
		Config: NewConfig(),
		client: http.DefaultClient,
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

// Run runs the Engine.
// Context errors may be returned.
func (e *Engine) Run(ctx context.Context) error {
	isTraining, err := isTrainingTask()
	if err != nil {
		return errors.Wrap(err, "isTrainingTask")
	}
	if e.testMode {
		go e.runTestConsole(ctx)
		e.logDebug("running subprocess for testing...")
		return e.runSubprocessOnly(ctx)
	}
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
		InputDir:                "/files/in",
		InputPattern:            e.Config.SelfDriving.InputPattern,
		WaitForReadyFiles:       e.Config.SelfDriving.WaitForReadyFiles,
	}
	processor := &selfdriving.Processor{
		Logger:           logger,
		Selector:         sel,
		OutputDirPattern: e.Config.SelfDriving.OutputDirPattern,
		MoveToDir:        "/files/out/completed",
		ErrDir:           "/files/out/errors",
		ResultsDir:       "/files/out/results",
		Process:          e.processSelfDrivingFile,
	}
	if err := processor.Run(ctx); err != nil {
		return errors.Wrap(err, "processor")
	}
	return nil
}

func (e *Engine) processSelfDrivingFile(outputDir string, file selfdriving.File) error {
	e.logDebug("processing file:", file)
	req, err := e.newRequestFromFile(e.Config.Webhooks.Process.URL, file)
	if err != nil {
		return errors.Wrap(err, "new request")
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		e.logDebug("no content output for file:", file.Path)
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
			outputFile := filepath.Join(outputDir, p.FileName())
			err = writeOutputFile(outputFile, p)
			if err != nil {
				return errors.Wrap(err, "writing output file")
			}
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
				chunkStartTimeMs := time.Now().Unix()
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
					if err := e.processMessage(ctx, msg, chunkStartTimeMs); err != nil {
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

func (e *Engine) processMessage(ctx context.Context, msg *sarama.ConsumerMessage, chunkStartTimeMs int64) error {
	fmt.Println("Partition: ", msg.Partition)
	fmt.Println("Offset: ", msg.Offset)
	topicMap := e.consumer.HighWaterMarks()[msg.Topic]
	currentLags := topicMap[msg.Partition] - msg.Offset - 1
	fmt.Printf("Current Lags: %+v\n", currentLags)
	messageInfo := fmt.Sprintf("%s:%d:%d:%d", msg.Topic, msg.Partition, msg.Offset, currentLags)
	start := time.Now()
	defer func() {
		now := time.Now()
		e.addProcessingTime(now.Sub(start))
	}()
	var typeCheck struct {
		Type messageType
	}
	if err := json.Unmarshal(msg.Value, &typeCheck); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	switch typeCheck.Type {
	case messageTypeMediaChunk:
		if err := e.processMessageMediaChunk(ctx, msg, start, messageInfo, chunkStartTimeMs); err != nil {
			return errors.Wrap(err, "process media chunk")
		}
	default:
		e.logDebug(fmt.Sprintf("ignoring message of type %q: %+v", typeCheck.Type, msg))
	}
	return nil
}

// processMessageMediaChunk processes a single media chunk as described by the sarama.ConsumerMessage.
func (e *Engine) processMessageMediaChunk(ctx context.Context, msg *sarama.ConsumerMessage, receivedChunkTime time.Time, messageInfo string, chunkStartTimeMs int64) error {
	var mediaChunk mediaChunkMessage
	// Add time receive chunk to infoMsg
	if err := json.Unmarshal(msg.Value, &mediaChunk); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	e.sendEvent(event{
		Key:         mediaChunk.ChunkUUID,
		Type:        eventConsumed,
		JobID:       mediaChunk.JobID,
		TaskID:      mediaChunk.TaskID,
		ChunkID:     mediaChunk.ChunkUUID,
		InstanceID:  e.Config.Engine.InstanceID,
		MessageInfo: messageInfo,
	})
	finalUpdateMessage := chunkResult{
		Type:      messageTypeChunkResult,
		TaskID:    mediaChunk.TaskID,
		ChunkUUID: mediaChunk.ChunkUUID,
		Status:    chunkStatusSuccess, // optimistic
	}

	var retResp map[string]string
	hasErrorProcessingMessage := false

	defer func() {
		chunkObject := &ChunkInfo{
			ChunkProcessedTimeInMs: time.Now().Unix() - chunkStartTimeMs,
			ChunkStatus:            string(finalUpdateMessage.Status),
		}
		chunkInfo, errInfo := json.Marshal(chunkObject)
		if errInfo != nil {
			fmt.Println("Cannot Marshal chunkInfo: " + errInfo.Error())
			chunkInfo = []byte("")
		}
		// Initial declaration info message
		infoMsgStruct := map[string]interface{}{
			mediaChunk.ChunkUUID: map[string]interface{}{
				"chunkIndex":                 mediaChunk.ChunkIndex,
				"getChunkTime":               receivedChunkTime.UTC().Format("2006-01-02T15:04:05Z"),
				"producingMessageResultTime": time.Now().UTC().Format("2006-01-02T15:04:05Z"),
				"startAwsServiceTime":        retResp["startAwsServiceTime"],
				"getResultAwsServiceTime":    retResp["getResultAwsServiceTime"],
				"errorMessage":               finalUpdateMessage.ErrorMsg,
			},
		}
		// send the final (ChunkResult) message
		infoMsg, errInfo := json.Marshal(infoMsgStruct)
		if errInfo != nil {
			fmt.Println("Cannot Marshal InfoMsg: " + errInfo.Error())
			infoMsg = []byte("")
		}
		if hasErrorProcessingMessage == false {
			finalUpdateMessage.InfoMsg = string(infoMsg)
		} else {
			finalUpdateMessage.ErrorMsg = string(infoMsg)
			finalUpdateMessage.FailureMsg = string(infoMsg)
		}
		finalUpdateMessage.TimestampUTC = time.Now().Unix()
		fmt.Printf("FinalUpdateMessage: %+v\n", finalUpdateMessage)
		_, _, err := e.producer.SendMessage(&sarama.ProducerMessage{
			Topic: e.Config.Kafka.ChunkTopic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: newJSONEncoder(finalUpdateMessage),
		})
		if err != nil {
			e.logDebug("WARN", "failed to send final chunk update:", err)
		}
		e.sendEvent(event{
			Key:         mediaChunk.ChunkUUID,
			Type:        eventProduced,
			JobID:       mediaChunk.JobID,
			TaskID:      mediaChunk.TaskID,
			ChunkID:     mediaChunk.ChunkUUID,
			InstanceID:  e.Config.Engine.InstanceID,
			MessageInfo: messageInfo,
			ChunkInfo:   string(chunkInfo),
		})
	}()
	ignoreChunk := false
	retry := newDoubleTimeBackoff(
		e.Config.Webhooks.Backoff.InitialBackoffDuration,
		e.Config.Webhooks.Backoff.MaxBackoffDuration,
		e.Config.Webhooks.Backoff.MaxRetries,
	)
	var content string
	err := retry.Do(func() error {
		req, err := e.newRequestFromMediaChunk(e.client, e.Config.Webhooks.Process.URL, mediaChunk)
		if err != nil {
			return errors.Wrap(err, "new request")
		}
		req = req.WithContext(ctx)
		resp, err := e.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			return errors.Wrap(err, "read body")
		}
		if resp.StatusCode == http.StatusNoContent {
			ignoreChunk = true
			return nil
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("%d: %s", resp.StatusCode, strings.TrimSpace(buf.String()))
		}
		if buf.Len() == 0 {
			ignoreChunk = true
			return nil
		}
		content = buf.String()
		return nil
	})
	if err != nil {
		hasErrorProcessingMessage = true
		// send error message
		finalUpdateMessage.Status = chunkStatusError
		finalUpdateMessage.ErrorMsg = err.Error()
		finalUpdateMessage.FailureReason = "internal_error"
		finalUpdateMessage.FailureMsg = finalUpdateMessage.ErrorMsg
		return err
	}
	if ignoreChunk {
		finalUpdateMessage.Status = chunkStatusIgnored
		return nil
	}
	// send output message
	outputMessage := mediaChunkMessage{
		Type:          messageTypeEngineOutput,
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

// jsonEncoder encodes JSON.
type jsonEncoder struct {
	v    interface{}
	once sync.Once
	b    []byte
	err  error
}

func newJSONEncoder(v interface{}) sarama.Encoder {
	return &jsonEncoder{v: v}
}

func (j *jsonEncoder) encode() {
	j.once.Do(func() {
		j.b, j.err = json.Marshal(j.v)
	})
}

func (j *jsonEncoder) Encode() ([]byte, error) {
	j.encode()
	return j.b, j.err
}

func (j *jsonEncoder) Length() int {
	j.encode()
	return len(j.b)
}
