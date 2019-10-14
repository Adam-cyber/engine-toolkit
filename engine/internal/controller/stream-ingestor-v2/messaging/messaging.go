package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/veritone/edge-messages"
)

const (
	defaultChunkTopic        = "chunk_all"
	defaultEngineStatusTopic = "engine_status"
	defaultEventsTopic       = "events"
)

type Client interface {
	ProduceRandom(ctx context.Context, topic string, key string, value []byte) (error, int32, int64)
	ProduceHash(ctx context.Context, topic string, key string, value []byte) (error, int32, int64)
	ProduceManual(ctx context.Context, topic string, partition int32, key string, value []byte) (error, int32, int64)
	Consume(ctx context.Context, topic string) <-chan ConsumerMessage
	ConsumeWithPartition(ctx context.Context, topic string, partition int32) <-chan ConsumerMessage
	Err() <-chan error
	Close() []error
}

type ConsumerMessage sarama.ConsumerMessage

type ClientConfig struct {
	MessageTopics MessageTopicConfig `json:"topics,omitempty"`
	Kafka         KafkaClientConfig  `json:"kafka,omitempty"`
}

type MessageTopicConfig struct {
	ChunkQueue   string `json:"chunkQueue,omitempty"`
	EngineStatus string `json:"engineHeartbeat,omitempty"`
	Events       string `json:"events,omitempty"`
}

func (m *MessageTopicConfig) defaults() {
	if m.ChunkQueue == "" {
		m.ChunkQueue = defaultChunkTopic
	}
	if m.EngineStatus == "" {
		m.EngineStatus = defaultEngineStatusTopic
	}
	if m.Events == "" {
		m.Events = defaultEventsTopic
	}
}

type MessageContext struct {
	AppName    string
	TDOID      string
	JobID      string
	TaskID     string
	EngineID   string
	InstanceID string
}

type Helper struct {
	Topics     MessageTopicConfig
	client     Client
	messageCtx MessageContext
}

func NewHelper(client Client, config ClientConfig, messageCtx MessageContext) *Helper {
	config.MessageTopics.defaults()

	return &Helper{
		Topics:     config.MessageTopics,
		client:     client,
		messageCtx: messageCtx,
	}
}

func (h *Helper) ProduceHeartbeatMessage(ctx context.Context, msg *messages.EngineHeartbeat) error {
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.TDOID = h.messageCtx.TDOID
	msg.EngineID = h.messageCtx.EngineID
	msg.EngineInstanceID = h.messageCtx.InstanceID

	return h.send(ctx, h.Topics.EngineStatus, -1, msg.Key(), msg)
}

func (h *Helper) ProduceMediaChunkMessage(ctx context.Context, msg *messages.MediaChunk) error {
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.TDOID = h.messageCtx.TDOID

	if err := h.send(ctx, h.Topics.ChunkQueue, -1, msg.Key(), msg); err != nil {
		return err
	}

	if err := h.produceEdgeEventMessage(ctx, messages.MediaChunkProduced, msg.ChunkUUID, msg.TimestampUTC); err != nil {
		return fmt.Errorf("failed to publish edgeEvent %s message to %s queue: %s", messages.MediaChunkProduced, h.Topics.Events, err)
	}
	return nil
}

func (h *Helper) ProduceChunkEOFMessage(ctx context.Context, msg *messages.ChunkEOF) error {
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.TDOID = h.messageCtx.TDOID

	if err := h.send(ctx, h.Topics.ChunkQueue, -1, msg.Key(), msg); err != nil {
		return err
	}

	if err := h.produceEdgeEventMessage(ctx, messages.ChunkEOFProduced, "", msg.TimestampUTC); err != nil {
		return fmt.Errorf("failed to publish edgeEvent %s message to %s queue: %s", messages.ChunkEOFProduced, h.Topics.Events, err)
	}
	return nil
}

func (h *Helper) ProduceStreamInitMessage(ctx context.Context, topic string, partition int32, msg *messages.StreamInit) error {
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.TDOID = h.messageCtx.TDOID

	return h.send(ctx, topic, partition, msg.Key(), msg)
}

func (h *Helper) ProduceStreamEOFMessage(ctx context.Context, topic string, partition int32, msg *messages.StreamEOF) error {
	return h.send(ctx, topic, partition, msg.Key(), msg)
}

func (h *Helper) ProduceRawStreamMessage(ctx context.Context, topic string, partition int32, msg *messages.RawStream) error {
	return h.send(ctx, topic, partition, msg.Key(), msg.Value)
}

func (h *Helper) produceEdgeEventMessage(ctx context.Context, eventType messages.EdgeEvent, chunkId string, eventTimeUTC int64) error {
	msg := messages.EmptyEdgeEventData()
	msg.Event = eventType
	msg.Component = h.messageCtx.AppName
	msg.ChunkID = chunkId
	msg.JobID = h.messageCtx.JobID
	msg.TaskID = h.messageCtx.TaskID
	msg.EventTimeUTC = eventTimeUTC
	msg.EngineInfo = &messages.EngineInfo{
		EngineID:   h.messageCtx.EngineID,
		InstanceID: h.messageCtx.InstanceID,
	}

	return h.send(ctx, h.Topics.Events, -1, msg.Key(), msg)
}

func (h *Helper) send(ctx context.Context, topic string, partition int32, key string, value interface{}) error {
	if h.client == nil {
		panic("message producer's client instance is nil")
	}

	valBytes, ok := value.([]byte)
	if !ok {
		var err error
		// if value is not a byte slice, JSON encode it
		valBytes, err = json.Marshal(value)
		if err != nil {
			return err
		}
	}

	// for invalid partition just call regular produce
	if partition < 0 {
		err, _, _ := h.client.ProduceRandom(ctx, topic, key, valBytes)
		return err
	}

	err, _, _ := h.client.ProduceManual(ctx, topic, partition, key, valBytes)
	return err
}

func ParseStreamTopic(topicStr string) (string, int32, string) {
	result := strings.Split(topicStr, ":")
	if len(result) < 2 {
		return "", -1, ""
	}

	partition, err := strconv.Atoi(result[1])
	if err != nil {
		return "", -1, ""
	}

	var prefix string
	if len(result) > 2 {
		prefix = result[2]
	}

	return result[0], int32(partition), prefix
}
