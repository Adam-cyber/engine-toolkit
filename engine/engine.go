package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Engine consumes messages and calls webhooks to
// fulfil the requests.
type Engine struct {
	producer      Producer
	eventProducer Producer
	consumer      Consumer

	testMode bool

	client   *http.Client
	logDebug func(args ...interface{})

	logContextLock sync.RWMutex
	logContext     *logContext

	// Config holds the Engine configuration.
	Config Config

	// processing time
	processingDurationLock sync.RWMutex
	processingDuration     time.Duration
}

// logContextSet sets the current log context.
func (e *Engine) logContextSet(c *logContext) {
	e.logContextLock.Lock()
	defer e.logContextLock.Unlock()
	e.logContext = c
}

// logContextClear clears the current log context.
func (e *Engine) logContextClear() {
	e.logContextLock.Lock()
	defer e.logContextLock.Unlock()
	e.logContext = nil
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

type writerFunc func(p []byte) (n int, err error)

func (fn writerFunc) Write(p []byte) (n int, err error) {
	return fn(p)
}

// runInference starts the subprocess and routes work to webhooks.
// This is used to process files.
func (e *Engine) runInference(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cmd *exec.Cmd
	if len(e.Config.Subprocess.Arguments) > 0 {
		writeLogFunc := writerFunc(func(p []byte) (n int, err error) {
			var lc *logContext
			e.logContextLock.RLock()
			lc = e.logContext
			e.logContextLock.RUnlock()
			if lc == nil {
				return len(p), nil
			}
			e.sendEvent(event{
				Key:     lc.Key,
				Type:    lc.Type,
				JobID:   lc.JobID,
				TaskID:  lc.TaskID,
				ChunkID: lc.ChunkID,
				LogText: string(p),
			})
			return len(p), nil
		})
		cmd = exec.CommandContext(ctx, e.Config.Subprocess.Arguments[0], e.Config.Subprocess.Arguments[1:]...)
		cmd.Stdout = io.MultiWriter(e.Config.Stdout, writeLogFunc)
		cmd.Stderr = io.MultiWriter(e.Config.Stderr, writeLogFunc)
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
	e.logDebug("waiting for messages...")
	e.sendEvent(event{
		Key:  e.Config.Engine.ID,
		Type: eventStart,
	})
	go e.sendPeriodicEvents(ctx)
	go func() {
		defer func() {
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
					return
				}
				//e.logDebug(fmt.Sprintf("message: topic:%q partition:%v offset:%v", msg.Topic, msg.Partition, msg.Offset))
				e.consumer.MarkOffset(msg, "")
				if err := e.processMessage(ctx, msg); err != nil {
					e.logDebug(fmt.Sprintf("processing error: %v", err))
				}
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
		Type messageType
	}
	if err := json.Unmarshal(msg.Value, &typeCheck); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	switch typeCheck.Type {
	case messageTypeMediaChunk:
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
	var mediaChunk mediaChunkMessage
	if err := json.Unmarshal(msg.Value, &mediaChunk); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	lc := &logContext{
		Key:     mediaChunk.ChunkUUID,
		Type:    eventLog,
		JobID:   mediaChunk.JobID,
		TaskID:  mediaChunk.TaskID,
		ChunkID: mediaChunk.ChunkUUID,
	}
	e.logContextSet(lc)
	defer e.logContextClear()
	e.sendEvent(event{
		Key:     mediaChunk.ChunkUUID,
		Type:    eventConsumed,
		JobID:   mediaChunk.JobID,
		TaskID:  mediaChunk.TaskID,
		ChunkID: mediaChunk.ChunkUUID,
	})
	finalUpdateMessage := chunkResult{
		Type:      messageTypeChunkResult,
		TaskID:    mediaChunk.TaskID,
		ChunkUUID: mediaChunk.ChunkUUID,
		Status:    chunkStatusSuccess, // optimistic
	}
	defer func() {
		// send the final (ChunkResult) message
		finalUpdateMessage.TimestampUTC = time.Now().Unix()
		_, _, err := e.producer.SendMessage(&sarama.ProducerMessage{
			Topic: e.Config.Kafka.ChunkTopic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: newJSONEncoder(finalUpdateMessage),
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
	retry := newDoubleTimeBackoff(
		e.Config.Webhooks.Backoff.InitialBackoffDuration,
		e.Config.Webhooks.Backoff.MaxBackoffDuration,
		e.Config.Webhooks.Backoff.MaxRetries,
	)
	var content string
	err := retry.Do(func() error {
		req, err := newRequestFromMediaChunk(e.client, e.Config.Webhooks.Process.URL, mediaChunk)
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

func nolog(args ...interface{}) {}

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

type logContext struct {
	Key     string
	Type    string
	JobID   string
	TaskID  string
	ChunkID string
}
