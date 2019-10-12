package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/matryer/is"
)

// TestProcessingChunk tests the entire end to end flow of processing
// a chunk.
func TestProcessingChunk(t *testing.T) {
	is := is.New(t)

	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{} // no subprocess
	engine.Config.Kafka.ChunkTopic = "chunk-topic"
	engine.Config.Engine.ID = "engineID"
	engine.Config.Events.PeriodicUpdateDuration = 0
	engine.logDebug = func(args ...interface{}) {
		log.Println(args...)
	}
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	outputEventsPipe := newPipe()
	defer outputEventsPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	engine.eventProducer = outputEventsPipe
	readySrv := newOKServer()
	defer readySrv.Close()
	engine.Config.Webhooks.Ready.URL = readySrv.URL
	processSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		engineOutput := engineOutput{
			Series: []seriesObject{
				{
					Object: object{
						Label: "something",
					},
				},
			},
		}
		err := json.NewEncoder(w).Encode(engineOutput)
		is.NoErr(err)
	}))
	defer processSrv.Close()
	engine.Config.Webhooks.Process.URL = processSrv.URL

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		err := engine.Run(ctx)
		is.NoErr(err)
	}()
	inputMessage := mediaChunkMessage{
		TimestampUTC:  time.Now().Unix(),
		ChunkUUID:     "123",
		Type:          messageTypeMediaChunk,
		StartOffsetMS: 1000,
		EndOffsetMS:   2000,
		JobID:         "job1",
		TDOID:         "tdo1",
		TaskID:        "task1",
	}
	_, _, err := inputPipe.SendMessage(&sarama.ProducerMessage{
		Offset: 1,
		Key:    sarama.StringEncoder(inputMessage.TaskID),
		Value:  newJSONEncoder(inputMessage),
	})
	is.NoErr(err)

	var outputMsg *sarama.ConsumerMessage
	var chunkResult chunkResult

	// read the chunk success message
	select {
	case outputMsg = <-outputPipe.Messages():
	case <-time.After(1 * time.Second):
		is.Fail() // timed out
		return
	}
	is.Equal(string(outputMsg.Key), inputMessage.TaskID)      // output message key must be TaskID
	is.Equal(outputMsg.Topic, engine.Config.Kafka.ChunkTopic) // chunk topic
	err = json.Unmarshal(outputMsg.Value, &chunkResult)
	is.NoErr(err)
	is.Equal(chunkResult.ErrorMsg, "")
	is.Equal(chunkResult.Type, messageTypeChunkResult)
	is.Equal(chunkResult.TaskID, inputMessage.TaskID)
	is.Equal(chunkResult.ChunkUUID, inputMessage.ChunkUUID)
	is.Equal(chunkResult.Status, chunkStatusSuccess)

	is.Equal(chunkResult.EngineOutput.Type, messageTypeEngineOutput)
	is.Equal(chunkResult.EngineOutput.TaskID, inputMessage.TaskID)
	is.Equal(chunkResult.EngineOutput.ChunkUUID, inputMessage.ChunkUUID)
	is.Equal(chunkResult.EngineOutput.StartOffsetMS, 1000)
	is.Equal(chunkResult.EngineOutput.EndOffsetMS, 2000)

	var output engineOutput
	err = json.Unmarshal([]byte(chunkResult.EngineOutput.Content), &output)
	is.NoErr(err)
	is.Equal(len(output.Series), 1)
	is.Equal(output.Series[0].Object.Label, "something")
	is.Equal(inputPipe.Offset, int64(1)) // Offset

	// stop the engine
	cancel()

	// events
	eventMsg, evt := popEvent(t, outputEventsPipe)
	is.Equal(string(eventMsg.Key), "engineID")
	is.Equal(evt.Type, eventType)   // event type
	is.Equal(evt.Event, eventStart) // event name

	eventMsg, evt = popEvent(t, outputEventsPipe)
	is.Equal(string(eventMsg.Key), inputMessage.ChunkUUID)
	is.Equal(evt.Type, eventType)                 // event type
	is.Equal(evt.Event, eventConsumed)            // event name
	is.Equal(evt.JobID, inputMessage.JobID)       // JobID
	is.Equal(evt.TaskID, inputMessage.TaskID)     // TaskID
	is.Equal(evt.ChunkID, inputMessage.ChunkUUID) // ChunkUUID

	eventMsg, evt = popEvent(t, outputEventsPipe)
	is.Equal(string(eventMsg.Key), inputMessage.ChunkUUID)
	is.Equal(evt.Type, eventType)                 // event type
	is.Equal(evt.Event, eventProduced)            // event name
	is.Equal(evt.JobID, inputMessage.JobID)       // JobID
	is.Equal(evt.TaskID, inputMessage.TaskID)     // TaskID
	is.Equal(evt.ChunkID, inputMessage.ChunkUUID) // ChunkUUID

	eventMsg, evt = popEvent(t, outputEventsPipe)
	is.Equal(string(eventMsg.Key), "engineID")
	is.Equal(evt.Type, eventType)  // event type
	is.Equal(evt.Event, eventStop) // event name
}

func popEvent(t *testing.T, outputEventsPipe *pipe) (*sarama.ConsumerMessage, *edgeEvent) {
	is := is.New(t)
	var evt edgeEvent
	var eventMsg *sarama.ConsumerMessage
	select {
	case eventMsg = <-outputEventsPipe.Messages():
	case <-time.After(1 * time.Second):
		is.Fail() // timed out waiting for event
		return eventMsg, &evt
	}
	err := json.Unmarshal(eventMsg.Value, &evt)
	is.NoErr(err)
	return eventMsg, &evt
}

// TestProcessingChunkError tests the entire end to end flow of processing
// a chunk simulating an error from the Process webhook.
func TestProcessingChunkError(t *testing.T) {
	is := is.New(t)

	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{} // no subprocess
	engine.Config.Kafka.ChunkTopic = "chunk-topic"
	engine.Config.Webhooks.Backoff.MaxRetries = 1
	engine.logDebug = func(args ...interface{}) {}
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	readySrv := newOKServer()
	defer readySrv.Close()
	engine.Config.Webhooks.Ready.URL = readySrv.URL
	processSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Something went wrong", http.StatusInternalServerError)
	}))
	defer processSrv.Close()
	engine.Config.Webhooks.Process.URL = processSrv.URL

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		err := engine.Run(ctx)
		is.NoErr(err)
	}()
	inputMessage := mediaChunkMessage{
		TimestampUTC:  time.Now().Unix(),
		ChunkUUID:     "123",
		Type:          messageTypeMediaChunk,
		StartOffsetMS: 1000,
		EndOffsetMS:   2000,
		JobID:         "job1",
		TDOID:         "tdo1",
		TaskID:        "task1",
	}
	_, _, err := inputPipe.SendMessage(&sarama.ProducerMessage{
		Offset: 1,
		Key:    sarama.StringEncoder(inputMessage.TaskID),
		Value:  newJSONEncoder(inputMessage),
	})
	is.NoErr(err)

	var outputMsg *sarama.ConsumerMessage
	var chunkResult chunkResult

	// read the chunk success message
	select {
	case outputMsg = <-outputPipe.Messages():
	case <-time.After(1 * time.Second):
		is.Fail() // timed out
		return
	}
	is.Equal(string(outputMsg.Key), inputMessage.TaskID)      // output message key must be TaskID
	is.Equal(outputMsg.Topic, engine.Config.Kafka.ChunkTopic) // chunk topic
	err = json.Unmarshal(outputMsg.Value, &chunkResult)
	is.NoErr(err)
	is.Equal(chunkResult.ErrorMsg, "500: Something went wrong")
	is.Equal(chunkResult.Type, messageTypeChunkResult)
	is.Equal(chunkResult.TaskID, inputMessage.TaskID)
	is.Equal(chunkResult.ChunkUUID, inputMessage.ChunkUUID)
	is.Equal(chunkResult.Status, chunkStatusError)
	is.Equal(chunkResult.FailureReason, "internal_error")
	is.Equal(chunkResult.FailureMsg, "500: Something went wrong")
	is.True(chunkResult.EngineOutput == nil)
}

func TestReadiness(t *testing.T) {
	is := is.New(t)
	var lock sync.Mutex
	var ready bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			ready = true
		}
	}))
	defer srv.Close()
	engine := NewEngine()
	engine.logDebug = func(args ...interface{}) {}
	engine.Config.Webhooks.Ready.URL = srv.URL
	engine.Config.Webhooks.Ready.PollDuration = 10 * time.Millisecond
	engine.Config.Webhooks.Ready.MaximumPollDuration = 1 * time.Second
	err := engine.ready(context.Background())
	is.NoErr(err)
}

func TestReadinessMaximumPollDuration(t *testing.T) {
	is := is.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	engine := NewEngine()
	engine.logDebug = func(args ...interface{}) {}
	engine.Config.Webhooks.Ready.URL = srv.URL
	engine.Config.Webhooks.Ready.PollDuration = 10 * time.Millisecond
	engine.Config.Webhooks.Ready.MaximumPollDuration = 100 * time.Millisecond
	err := engine.ready(context.Background())
	is.Equal(err, errReadyTimeout)
}

func TestReadinessContextCancelled(t *testing.T) {
	is := is.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	engine := NewEngine()
	engine.logDebug = func(args ...interface{}) {}
	engine.Config.Webhooks.Ready.URL = srv.URL
	engine.Config.Webhooks.Ready.PollDuration = 10 * time.Millisecond
	engine.Config.Webhooks.Ready.MaximumPollDuration = 1 * time.Second
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err := engine.ready(ctx)
	is.Equal(err, context.DeadlineExceeded)

}

func TestSubprocess(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{"echo", "-n", "something"}
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	readySrv := newOKServer()
	defer readySrv.Close()
	engine.Config.Webhooks.Ready.URL = readySrv.URL

	var buf bytes.Buffer
	engine.Config.Stdout = &buf

	// engine will run until the subprocess exits
	err := engine.Run(ctx)
	is.NoErr(err)
	is.Equal(buf.String(), `something`)
}

func TestEndIfIdleDuration(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{} // no subprocess
	engine.Config.Engine.EndIfIdleDuration = 100 * time.Millisecond
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	err := engine.Run(ctx)
	is.NoErr(err)
}

func TestEndAfterDuration(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{} // no subprocess
	engine.Config.Engine.EndAfterDuration = 100 * time.Millisecond
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	err := engine.Run(ctx)
	is.NoErr(err)
}

func TestEndAfterDurationWithNoConfig(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{} // no subprocess
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	err := engine.Run(ctx)
	is.NoErr(err)
}

func newOKServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
}

func TestIsTrainingTask(t *testing.T) {
	is := is.New(t)

	os.Setenv("PAYLOAD_FILE", "")
	isTraining, err := isTrainingTask()
	is.NoErr(err)
	is.Equal(isTraining, false) // not training

	os.Setenv("PAYLOAD_FILE", "testdata/training-task-payload.json")
	isTraining, err = isTrainingTask()
	is.NoErr(err)
	is.Equal(isTraining, true) // training task

	os.Setenv("PAYLOAD_FILE", "testdata/processing-task-payload.json")
	isTraining, err = isTrainingTask()
	is.NoErr(err)
	is.Equal(isTraining, false) // not training task
}

func TestIgnoredChunks(t *testing.T) {
	is := is.New(t)

	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{} // no subprocess
	engine.Config.Kafka.ChunkTopic = "chunk-topic"
	engine.logDebug = func(args ...interface{}) {}
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe
	readySrv := newOKServer()
	defer readySrv.Close()
	engine.Config.Webhooks.Ready.URL = readySrv.URL
	processSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer processSrv.Close()
	engine.Config.Webhooks.Process.URL = processSrv.URL

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		err := engine.Run(ctx)
		is.NoErr(err)
	}()

	inputMessage := mediaChunkMessage{
		TimestampUTC:  time.Now().Unix(),
		ChunkUUID:     "123",
		Type:          messageTypeMediaChunk,
		StartOffsetMS: 1000,
		EndOffsetMS:   2000,
		JobID:         "job1",
		TDOID:         "tdo1",
		TaskID:        "task1",
	}
	_, _, err := inputPipe.SendMessage(&sarama.ProducerMessage{
		Offset: 1,
		Key:    sarama.StringEncoder(inputMessage.TaskID),
		Value:  newJSONEncoder(inputMessage),
	})
	is.NoErr(err)

	var outputMsg *sarama.ConsumerMessage
	var chunkProcessedStatus chunkProcessedStatus

	// read the chunk ignore message
	select {
	case outputMsg = <-outputPipe.Messages():
	case <-time.After(1 * time.Second):
		is.Fail() // timed out
		return
	}
	is.Equal(string(outputMsg.Key), inputMessage.TaskID)      // output message key must be TaskID
	is.Equal(outputMsg.Topic, engine.Config.Kafka.ChunkTopic) // chunk topic
	err = json.Unmarshal(outputMsg.Value, &chunkProcessedStatus)
	is.NoErr(err)
	is.Equal(chunkProcessedStatus.ErrorMsg, "")
	is.Equal(chunkProcessedStatus.Type, messageTypeChunkResult)
	is.Equal(chunkProcessedStatus.TaskID, inputMessage.TaskID)
	is.Equal(chunkProcessedStatus.ChunkUUID, inputMessage.ChunkUUID)
	is.Equal(chunkProcessedStatus.Status, chunkStatusIgnored)

	is.Equal(inputPipe.Offset, int64(1)) // Offset
}

func TestSubprocessCrash(t *testing.T) {
	is := is.New(t)

	engine := NewEngine()
	engine.Config.Subprocess.Arguments = []string{"./testdata/subprocesses/crash.sh"}
	engine.Config.Kafka.ChunkTopic = "chunk-topic"
	engine.logDebug = func(args ...interface{}) {
		log.Println(args...)
	}
	readySrv := newOKServer()
	defer readySrv.Close()
	engine.Config.Webhooks.Ready.URL = readySrv.URL
	inputPipe := newPipe()
	defer inputPipe.Close()
	outputPipe := newPipe()
	defer outputPipe.Close()
	engine.consumer = inputPipe
	engine.producer = outputPipe

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := engine.Run(ctx)
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "exit status 123"))

}

type engineOutput struct {
	// SourceEngineID   string         `json:"sourceEngineId,omitempty"`
	// SourceEngineName string         `json:"sourceEngineName,omitempty"`
	// TaskPayload      payload        `json:"taskPayload,omitempty"`
	// TaskID           string         `json:"taskId"`
	// EntityID         string         `json:"entityId,omitempty"`
	// LibraryID        string         `json:"libraryId"`
	Series []seriesObject `json:"series"`
}

type seriesObject struct {
	Start     int    `json:"startTimeMs"`
	End       int    `json:"stopTimeMs"`
	EntityID  string `json:"entityId"`
	LibraryID string `json:"libraryId"`
	Object    object `json:"object"`
}

type object struct {
	Label        string   `json:"label"`
	Text         string   `json:"text"`
	ObjectType   string   `json:"type"`
	URI          string   `json:"uri"`
	EntityID     string   `json:"entityId,omitempty"`
	LibraryID    string   `json:"libraryId,omitempty"`
	Confidence   float64  `json:"confidence"`
	BoundingPoly []coords `json:"boundingPoly"`
}

type coords struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}
