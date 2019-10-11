package messaging

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	messages "github.com/veritone/edge-messages"
)

func TestMessaging(t *testing.T) {
	suite.Run(t, new(messagingTestSuite))
	suite.Run(t, new(kafkaTestSuite))
	suite.Run(t, new(messagingMockClientTestSuite))
}

type messagingTestSuite struct {
	suite.Suite
	hosts         []string
	topic         string
	numPartitions int32
}

func (t *messagingTestSuite) SetupSuite() {
	kafkaClient := new(kafkaTestSuite)
	t.hosts = []string{"kafka1:9092", "kafka2:9092"}
	t.topic = "test_topic"
	t.numPartitions = 20
	var err error

	err = kafkaClient.createTopic(t.hosts, t.topic, t.numPartitions)
	if err != nil {
		assert.Contains(t.T(), err.Error(), "Topic with this name already exists")
	}
}

func (t *messagingTestSuite) TestRandomPartitionerType() {
	t.SetupSuite()

	ctx := context.Background()
	timeToTun := 4
	partitions := []int32{}
	partitionsNotExpected := []int32{}
	topic := t.topic
	cfg := KafkaClientConfig{
		Brokers: t.hosts,
	}
	client, err := NewKafkaClient(cfg)
	msgValue := []byte(`{"type":"media_chunk","timestampUTC":1563972994087,"taskId":"taskId0","tdoId":"590575621","jobId":"jobId0","chunkIndex":1,"startOffsetMs":0,"endOffsetMs":300002,"mimeType":"audio/wav","cacheURI":"cacheURI0","taskPayload":{"applicationId":"64918cd5-5c95-4140-ab5a-2c39a733ea34","jobId":"jobId0","organizationId":"00000","recordingId":"00000000","taskId":"taskId0","taskPayload":{"organizationId":00000},"token":"token0","veritoneApiBaseUrl":"https://api.veritone.com"},"chunkUUID":"f2720054-97ce-4e89-94ea-82d82eec14d0"}`)
	assert.Nil(t.T(), err, "should create NewKafkaClient successfully")
	if err != nil || client == nil {
		return
	}
	for i := 0; i < timeToTun; i++ {
		err, partition, _ := client.ProduceRandom(ctx, topic, fmt.Sprintf("key%v", i), msgValue)
		assert.Nil(t.T(), err, fmt.Sprintf("[%v] should send kafka message successfully", i))
		partitions = append(partitions, partition)
		partitionsNotExpected = append(partitionsNotExpected, int32(i))
	}
	assert.NotEqual(t.T(), partitionsNotExpected, partitions, "The current Partitioner type is NewRoundRobinPartitioner")
	client.Close()
}

type MockKafkaClient struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *MockKafkaClient) Close() []error {
	ret := _m.Called()

	var r0 []error
	if rf, ok := ret.Get(0).(func() []error); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]error)
		}
	}

	return r0
}

// Consume provides a mock function with given fields: ctx, topic
func (_m *MockKafkaClient) Consume(ctx context.Context, topic string) <-chan ConsumerMessage {
	ret := _m.Called(ctx, topic)

	var r0 <-chan ConsumerMessage
	if rf, ok := ret.Get(0).(func(context.Context, string) <-chan ConsumerMessage); ok {
		r0 = rf(ctx, topic)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan ConsumerMessage)
		}
	}

	return r0
}

// ConsumeWithPartition provides a mock function with given fields: ctx, topic, partition
func (_m *MockKafkaClient) ConsumeWithPartition(ctx context.Context, topic string, partition int32) <-chan ConsumerMessage {
	ret := _m.Called(ctx, topic, partition)

	var r0 <-chan ConsumerMessage
	if rf, ok := ret.Get(0).(func(context.Context, string, int32) <-chan ConsumerMessage); ok {
		r0 = rf(ctx, topic, partition)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan ConsumerMessage)
		}
	}

	return r0
}

// Err provides a mock function with given fields:
func (_m *MockKafkaClient) Err() <-chan error {
	ret := _m.Called()

	var r0 <-chan error
	if rf, ok := ret.Get(0).(func() <-chan error); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan error)
		}
	}

	return r0
}

// ProduceHash provides a mock function with given fields: ctx, topic, key, value
func (_m *MockKafkaClient) ProduceHash(ctx context.Context, topic string, key string, value []byte) (error, int32, int64) {
	ret := _m.Called(ctx, topic, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, []byte) error); ok {
		r0 = rf(ctx, topic, key, value)
	} else {
		r0 = ret.Error(0)
	}

	var r1 int32
	if rf, ok := ret.Get(1).(func(context.Context, string, string, []byte) int32); ok {
		r1 = rf(ctx, topic, key, value)
	} else {
		r1 = ret.Get(1).(int32)
	}

	var r2 int64
	if rf, ok := ret.Get(2).(func(context.Context, string, string, []byte) int64); ok {
		r2 = rf(ctx, topic, key, value)
	} else {
		r2 = ret.Get(2).(int64)
	}

	return r0, r1, r2
}

// ProduceManual provides a mock function with given fields: ctx, topic, partition, key, value
func (_m *MockKafkaClient) ProduceManual(ctx context.Context, topic string, partition int32, key string, value []byte) (error, int32, int64) {
	ret := _m.Called(ctx, topic, partition, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, int32, string, []byte) error); ok {
		r0 = rf(ctx, topic, partition, key, value)
	} else {
		r0 = ret.Error(0)
	}

	var r1 int32
	if rf, ok := ret.Get(1).(func(context.Context, string, int32, string, []byte) int32); ok {
		r1 = rf(ctx, topic, partition, key, value)
	} else {
		r1 = ret.Get(1).(int32)
	}

	var r2 int64
	if rf, ok := ret.Get(2).(func(context.Context, string, int32, string, []byte) int64); ok {
		r2 = rf(ctx, topic, partition, key, value)
	} else {
		r2 = ret.Get(2).(int64)
	}

	return r0, r1, r2
}

// ProduceRandom provides a mock function with given fields: ctx, topic, key, value
func (_m *MockKafkaClient) ProduceRandom(ctx context.Context, topic string, key string, value []byte) (error, int32, int64) {
	ret := _m.Called(ctx, topic, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, []byte) error); ok {
		r0 = rf(ctx, topic, key, value)
	} else {
		r0 = ret.Error(0)
	}

	var r1 int32
	if rf, ok := ret.Get(1).(func(context.Context, string, string, []byte) int32); ok {
		r1 = rf(ctx, topic, key, value)
	} else {
		r1 = ret.Get(1).(int32)
	}

	var r2 int64
	if rf, ok := ret.Get(2).(func(context.Context, string, string, []byte) int64); ok {
		r2 = rf(ctx, topic, key, value)
	} else {
		r2 = ret.Get(2).(int64)
	}

	return r0, r1, r2
}

type messagingMockClientTestSuite struct {
	suite.Suite
	client *MockKafkaClient
	ctx    context.Context
	helper *Helper
}

func (t *messagingMockClientTestSuite) SetupSuite() {
	t.ctx = context.Background()

	t.client = new(MockKafkaClient)
	t.helper = NewHelper(t.client,
		ClientConfig{
			MessageTopics: MessageTopicConfig{
				ChunkQueue:   "chunk",
				EngineStatus: "status",
				Events:       "events",
			},
		},
		MessageContext{
			InstanceID: "id1",
			AppName:    "app",
			JobID:      "job1",
		})

	t.client.On("ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything).Return(nil, int32(0), int64(0))
}

func (t *messagingMockClientTestSuite) TestProduceHeartbeatMessage() {

	t.SetupSuite()

	msg := messages.EngineHeartbeat{}
	err := t.helper.ProduceHeartbeatMessage(t.ctx, &msg)
	t.Nil(err)

	if t.client.AssertCalled(t.T(), "ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything) {
		for _, call := range t.client.Calls {
			if call.Method == "ProduceRandom" {
				topic := call.Arguments.String(1)
				if topic == t.helper.Topics.EngineStatus {
					msgKey := call.Arguments.String(2)
					t.Equal(t.helper.messageCtx.InstanceID, msgKey)
					return
				}
			}
		}
	}

	t.FailNow("No expected call.")
}

func (t *messagingMockClientTestSuite) TestProduceMediaChunkMessage() {

	t.SetupSuite()

	msg := messages.MediaChunk{}
	err := t.helper.ProduceMediaChunkMessage(t.ctx, &msg)
	t.Nil(err)

	expecteMsgs := map[string]string{
		t.helper.Topics.ChunkQueue: "",
		t.helper.Topics.Events:     fmt.Sprintf("%s-%s-%s", messages.MediaChunkProduced, "app", "job1"),
	}

	if t.client.AssertCalled(t.T(), "ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything) {
		actualMsgs := make(map[string]string)
		for _, call := range t.client.Calls {
			if call.Method == "ProduceRandom" {
				topic := call.Arguments.String(1)
				msgKey := call.Arguments.String(2)
				actualMsgs[topic] = msgKey
			}
		}
		t.Equal(expecteMsgs, actualMsgs)
	}
}

func (t *messagingMockClientTestSuite) TestProduceChunkEOFMessage() {

	t.SetupSuite()

	msg := messages.ChunkEOF{}
	err := t.helper.ProduceChunkEOFMessage(t.ctx, &msg)
	t.Nil(err)

	expecteMsgs := map[string]string{
		t.helper.Topics.ChunkQueue: "",
		t.helper.Topics.Events:     fmt.Sprintf("%s-%s-%s", messages.ChunkEOFProduced, "app", "job1"),
	}

	if t.client.AssertCalled(t.T(), "ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything) {
		actualMsgs := make(map[string]string)
		for _, call := range t.client.Calls {
			if call.Method == "ProduceRandom" {
				topic := call.Arguments.String(1)
				msgKey := call.Arguments.String(2)
				actualMsgs[topic] = msgKey
			}
		}
		t.Equal(expecteMsgs, actualMsgs)
	}
}

func (t *messagingMockClientTestSuite) TestProduceStreamInitMessage() {

	t.SetupSuite()

	msg := messages.StreamInit{}

	err := t.helper.ProduceStreamInitMessage(t.ctx, "topic1", -1, &msg)
	t.Nil(err)

	if t.client.AssertCalled(t.T(), "ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything) {
		for _, call := range t.client.Calls {
			if call.Method == "ProduceRandom" {
				topic := call.Arguments.String(1)
				if topic == "topic1" {
					msgKey := call.Arguments.String(2)
					t.Equal("stream_init", msgKey)
					return
				}
			}
		}
	}

	t.FailNow("No expected call.")
}

func (t *messagingMockClientTestSuite) TestProduceStreamEOFMessage() {

	t.SetupSuite()

	msg := messages.StreamEOF{}

	err := t.helper.ProduceStreamEOFMessage(t.ctx, "topic1", -1, &msg)
	t.Nil(err)

	if t.client.AssertCalled(t.T(), "ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything) {
		for _, call := range t.client.Calls {
			if call.Method == "ProduceRandom" {
				topic := call.Arguments.String(1)
				if topic == "topic1" {
					msgKey := call.Arguments.String(2)
					t.Equal("stream_eof", msgKey)
					return
				}
			}
		}
	}

	t.FailNow("No expected call.")
}

func (t *messagingMockClientTestSuite) TestProduceRawStreamMessage() {

	t.SetupSuite()

	msg := messages.RawStream{}

	err := t.helper.ProduceRawStreamMessage(t.ctx, "topic1", -1, &msg)
	t.Nil(err)

	if t.client.AssertCalled(t.T(), "ProduceRandom", t.ctx, mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything) {
		for _, call := range t.client.Calls {
			if call.Method == "ProduceRandom" {
				topic := call.Arguments.String(1)
				if topic == "topic1" {
					msgKey := call.Arguments.String(2)
					t.Equal("raw_stream", msgKey)
					return
				}
			}
		}
	}

	t.FailNow("No expected call.")
}
