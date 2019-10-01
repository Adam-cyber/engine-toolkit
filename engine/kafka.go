package main

import (
	"encoding/json"
	"io"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

// Consumer consumes incoming messages.
type Consumer interface {
	io.Closer
	// Messages gets a channel on which sarama.ConsumerMessage
	// objets are sent. Closed when the Consumer is closed.
	Messages() <-chan *sarama.ConsumerMessage
	// MarkOffset indicates that this message has been processed.
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	HighWaterMarks() map[string]map[int32]int64
}

// Producer produces outgoing messages.
type Producer interface {
	io.Closer
	// SendMessage sends a sarama.ProducerMessage.
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
}

func newKafkaConsumer(brokers []string, group, topic string) (Consumer, func(), error) {
	cleanup := func() {}
	config := cluster.NewConfig()
	config.Version = sarama.V1_1_0_0
	config.ClientID = "veritone.engine-toolkit"
	config.Consumer.Return.Errors = true
	config.Consumer.Retry.Backoff = 1 * time.Second
	config.Consumer.Offsets.Retry.Max = 5
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 1 * time.Second
	config.Group.Mode = cluster.ConsumerModeMultiplex
	config.Group.PartitionStrategy = cluster.StrategyRoundRobin
	if err := config.Validate(); err != nil {
		return nil, cleanup, errors.Wrap(err, "config")
	}
	client, err := cluster.NewClient(brokers, config)
	if err != nil {
		return nil, cleanup, err
	}
	cleanup = func() {
		if err := client.Close(); err != nil {
			log.Println("kafka: consumer: client.Close:", err)
		}
	}
	consumer, err := cluster.NewConsumerFromClient(client, group, []string{topic})
	if err != nil {
		return nil, cleanup, errors.Wrapf(err, "consumer (brokers: %s, group: %s, topic: %s)", strings.Join(brokers, ", "), group, topic)
	}
	go func() {
		for err := range consumer.Errors() {
			log.Println("kafka: consumer:", err)
		}
	}()
	return consumer, cleanup, nil
}

func newKafkaProducer(brokers []string) (Producer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V1_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.MaxMessageBytes = 15 << (10 * 2) // 15 MB
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "config")
	}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, errors.Wrapf(err, "producer (brokers: %s)", strings.Join(brokers, ", "))
	}
	return producer, nil
}

type mediaChunkMessage struct {
	Type          messageType     `json:"type"`
	TimestampUTC  int64           `json:"timestampUTC"`
	MIMEType      string          `json:"mimeType"`
	TaskID        string          `json:"taskId"`
	TDOID         string          `json:"tdoId"`
	JobID         string          `json:"jobId"`
	ChunkIndex    int             `json:"chunkIndex"`
	StartOffsetMS int             `json:"startOffsetMs"`
	EndOffsetMS   int             `json:"endOffsetMs"`
	Width         int             `json:"width"`
	Height        int             `json:"height"`
	CacheURI      string          `json:"cacheURI"`
	Content       string          `json:"content"`
	TaskPayload   json.RawMessage `json:"taskPayload"`
	ChunkUUID     string          `json:"chunkUUID"`
}

// unmarshalPayload unmarshals the TaskPayload into the known
// fields (in the payload type). This will ignore custom fields.
func (m *mediaChunkMessage) unmarshalPayload() (payload, error) {
	var p payload
	if err := json.Unmarshal(m.TaskPayload, &p); err != nil {
		return p, err
	}
	return p, nil
}

// messageType is an enum type for edge message types.
type messageType string

// chunkStatus is an enum type for chunk statuses.
type chunkStatus string

const (
	messageTypeChunkResult  messageType = "chunk_result"
	messageTypeMediaChunk   messageType = "media_chunk"
	messageTypeEngineOutput messageType = "engine_output"
)

const (
	// chunkStatusSuccess is status to report when input chunk processed successfully
	chunkStatusSuccess chunkStatus = "SUCCESS"
	// chunkStatusError is status to report when input chunk was processed with error
	chunkStatusError chunkStatus = "ERROR"
	// chunkStatusIgnored is status to report when input chunk was ignored and not attempted processing (i.e. not of the expected type)
	chunkStatusIgnored chunkStatus = "IGNORED"
)

// chunkProcessedStatus - processing status of a chunk by stateless engines/conductors
type chunkProcessedStatus struct {
	Type         messageType `json:"type,omitempty"`
	TimestampUTC int64       `json:"timestampUTC,omitempty"`
	TaskID       string      `json:"taskId,omitempty"`
	ChunkUUID    string      `json:"chunkUUID,omitempty"`
	Status       chunkStatus `json:"status,omitempty"`
	ErrorMsg     string      `json:"errorMsg,omitempty"`
	InfoMsg      string      `json:"infoMsg,omitempty"`
}

type chunkResult struct {
	Type         messageType        `json:"type,omitempty"`
	TimestampUTC int64              `json:"timestampUTC,omitempty"`
	TaskID       string             `json:"taskId,omitempty"`
	ChunkUUID    string             `json:"chunkUUID,omitempty"`
	Status       chunkStatus        `json:"status,omitempty"`
	InfoMsg      string             `json:"infoMsg,omitempty"`
	EngineOutput *mediaChunkMessage `json:"engineOutput,omitempty"`

	ErrorMsg      string `json:"errorMsg,omitempty"`
	FailureReason string `json:"failureReason,omitempty"`
	FailureMsg    string `json:"failureMsg,omitempty"`
}

type payload struct {
	ApplicationID        string `json:"applicationId"`
	JobID                string `json:"jobId"`
	TaskID               string `json:"taskId"`
	RecordingID          string `json:"recordingId"`
	Token                string `json:"token"`
	Mode                 string `json:"mode,omitempty"`
	LibraryID            string `json:"libraryId"`
	LibraryEngineModelID string `json:"libraryEngineModelId"`
	StartFromEntity      string `json:"startFromEntity"`
	VeritoneAPIBaseURL   string `json:"veritoneApiBaseUrl"`
}
