package streamio

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"time"

	messages "github.com/veritone/edge-messages"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/veritone/edge-stream-ingestor/messaging"
	msgMocks "github.com/veritone/edge-stream-ingestor/messaging/mocks"
)

type messageTestSuite struct {
	suite.Suite
	messageHelper *messaging.Helper
	messageClient *msgMocks.Client
}

func (t *messageTestSuite) SetupTest() {
	var mcfg messaging.ClientConfig
	mctx := messaging.MessageContext{
		TaskID: "task1",
		JobID:  "job1",
		TDOID:  "tdo1",
	}
	t.messageClient = new(msgMocks.Client)
	t.messageHelper = messaging.NewHelper(t.messageClient, mcfg, mctx)
}

func (t *messageTestSuite) TestInitMessageStreamWriter() {
	config := MessageStreamWriterConfig{
		Topic:          "stream_1",
		Partition:      int32(100),
		KeyPrefix:      "prefix__",
		MaxMessageSize: 1024 * 5,
		NoDataTimeout:  time.Second * 10,
	}

	sw := NewMessageStreamWriter(t.messageHelper, config)
	if assert.IsType(t.T(), new(messageWriter), sw) {
		writer := sw.(*messageWriter)
		assert.Equal(t.T(), config, writer.config)
		assert.NotNil(t.T(), writer.bytesWritten)
	}
}

func (t *messageTestSuite) TestWriteStream() {
	ctx := context.Background()
	config := MessageStreamWriterConfig{
		Topic:          "stream_1",
		Partition:      int32(100),
		KeyPrefix:      "prefix__",
		MaxMessageSize: 1024,
	}
	sw := NewMessageStreamWriter(t.messageHelper, config)
	payload := make([]byte, 5*1024)
	stream := NewStreamFromReader(bytes.NewReader(payload))
	stream.MimeType = "video/mp4"
	stream.SetFfmpegFormat("mp4;vcodec=h264;acodec=aac")
	stream.StartOffsetMS = 20000
	stream.StartTime = time.Now()

	t.messageClient.On("ProduceManual", ctx, config.Topic, config.Partition, mock.AnythingOfType("string"), mock.Anything).Return(nil, int32(0), int64(0))

	if assert.NoError(t.T(), sw.WriteStream(ctx, stream)) {
		// 7 calls: stream_init, 5 * raw_stream, stream_eof
		if t.messageClient.AssertNumberOfCalls(t.T(), "ProduceManual", 7) {
			c := t.messageClient.Calls
			c[0].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__stream_init", mock.Anything)
			c[1].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[2].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[3].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[4].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[5].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__raw_stream", mock.Anything)
			c[6].Arguments.Assert(t.T(), ctx, config.Topic, config.Partition, "prefix__stream_eof", mock.Anything)

			// validate stream_init message
			valueArg, _ := c[0].Arguments.Get(4).([]byte)
			var streamInitMsg messages.StreamInit
			if assert.NoError(t.T(), json.Unmarshal(valueArg, &streamInitMsg)) {
				assert.Equal(t.T(), "task1", streamInitMsg.TaskID)
				assert.Equal(t.T(), "job1", streamInitMsg.JobID)
				assert.Equal(t.T(), "tdo1", streamInitMsg.TDOID)
				assert.Equal(t.T(), stream.StartOffsetMS, streamInitMsg.OffsetMS)
				assert.Equal(t.T(), stream.MimeType, streamInitMsg.MimeType)
				assert.Equal(t.T(), "mp4;vcodec=h264;acodec=aac", streamInitMsg.FfmpegFormat)
				assert.Equal(t.T(), stream.StartTime.Unix(), streamInitMsg.MediaStartTimeUTC)
			}

			// validate raw_stream message value
			for i := 1; i < 6; i++ {
				valueArg, _ := c[i].Arguments.Get(4).([]byte)
				start, end := (i-1)*1024, i*1024
				assert.Equal(t.T(), payload[start:end], valueArg)
			}

			// validate stream_eof message
			valueArg, _ = c[6].Arguments.Get(4).([]byte)
			var streamEOFMsg messages.StreamEOF
			if assert.NoError(t.T(), json.Unmarshal(valueArg, &streamEOFMsg)) {
				assert.Equal(t.T(), int64(5*1024), streamEOFMsg.ContentLength)
			}
		}
	}
}

func (t *messageTestSuite) TestWriteStreamTimeout() {
	ctx := context.Background()
	config := MessageStreamWriterConfig{
		Topic:          "stream_1",
		Partition:      int32(100),
		MaxMessageSize: 1024,
		NoDataTimeout:  time.Millisecond * 250,
	}
	sw := NewMessageStreamWriter(t.messageHelper, config)
	stream := NewStream()

	t.messageClient.On("ProduceManual", ctx, config.Topic, config.Partition, mock.AnythingOfType("string"), mock.Anything).Return(nil, int32(0), int64(0))

	err := sw.WriteStream(ctx, stream)
	if assert.Error(t.T(), err) {
		assert.Equal(t.T(), ErrWriterNoDataTimeout, err)
	}
}

func (t *messageTestSuite) TestReadStreamNoPrefix() {
	ctx := context.Background()
	config := MessageReaderConfig{
		Topic:     "stream_1",
		Partition: int32(100),
	}

	messagec := make(chan messaging.ConsumerMessage, 9)
	var ret <-chan messaging.ConsumerMessage = messagec
	t.messageClient.On("ConsumeWithPartition", mock.Anything, config.Topic, config.Partition).Return(ret)
	t.messageClient.On("Err").Return(make(<-chan error))

	streamInit := messages.RandomStreamInit("")
	streamInitValue, _ := json.Marshal(streamInit)
	messagec <- messaging.ConsumerMessage{
		Key:   []byte(streamInit.Key()),
		Value: streamInitValue,
	}

	streamContents := "abcdefghijklmnopqrstuvwxyz"
	streamBuf := bytes.NewBufferString(streamContents)

	for i := 0; i < len(streamContents); i += 4 {
		msg := messages.EmptyRawStream("")
		p := make([]byte, 4)
		n, err := streamBuf.Read(p)
		if assert.NoError(t.T(), err) {
			messagec <- messaging.ConsumerMessage{
				Key:   []byte(msg.Key()),
				Value: p[:n],
			}
		}
	}

	streamEOF := messages.RandomStreamEOF("")
	streamEOFValue, _ := json.Marshal(streamEOF)
	messagec <- messaging.ConsumerMessage{
		Key:   []byte(streamEOF.Key()),
		Value: streamEOFValue,
	}

	sr, err := NewMessageStreamReader(config, t.messageClient)
	if assert.NoError(t.T(), err) && assert.NotNil(t.T(), sr) {
		stream := sr.Stream(ctx)
		c, err := ioutil.ReadAll(stream)
		if assert.NoError(t.T(), err) {
			if assert.Len(t.T(), c, len(streamContents)) {
				assert.Equal(t.T(), streamContents, string(c))
			}
		}

		assert.Equal(t.T(), streamInit.MimeType, stream.MimeType)
		assert.Equal(t.T(), streamInit.FfmpegFormat, stream.FfmpegFormat.String())
		assert.Equal(t.T(), streamInit.OffsetMS, stream.StartOffsetMS)
		assert.Equal(t.T(), time.Unix(streamInit.MediaStartTimeUTC, 0), stream.StartTime)
	}
}

func (t *messageTestSuite) TestReadStreamWithPrefix() {
	ctx := context.Background()
	config := MessageReaderConfig{
		Topic:     "stream_1",
		Partition: int32(100),
		KeyPrefix: "prefix__",
	}

	messagec := make(chan messaging.ConsumerMessage, 9)
	var ret <-chan messaging.ConsumerMessage = messagec
	t.messageClient.On("ConsumeWithPartition", mock.Anything, config.Topic, config.Partition).Return(ret)
	t.messageClient.On("Err").Return(make(<-chan error))

	streamInit := messages.RandomStreamInit(config.KeyPrefix)
	streamInitValue, _ := json.Marshal(streamInit)
	messagec <- messaging.ConsumerMessage{
		Key:   []byte(streamInit.Key()),
		Value: streamInitValue,
	}

	streamContents := "abcdefghijklmnopqrstuvwxyz"
	streamBuf := bytes.NewBufferString(streamContents)

	for i := 0; i < len(streamContents); i += 4 {
		msg := messages.EmptyRawStream(config.KeyPrefix)
		p := make([]byte, 4)
		n, err := streamBuf.Read(p)
		if assert.NoError(t.T(), err) {
			messagec <- messaging.ConsumerMessage{
				Key:   []byte(msg.Key()),
				Value: p[:n],
			}
		}
	}

	streamEOF := messages.RandomStreamEOF(config.KeyPrefix)
	streamEOFValue, _ := json.Marshal(streamEOF)
	messagec <- messaging.ConsumerMessage{
		Key:   []byte(streamEOF.Key()),
		Value: streamEOFValue,
	}

	sr, err := NewMessageStreamReader(config, t.messageClient)
	if assert.NoError(t.T(), err) && assert.NotNil(t.T(), sr) {
		stream := sr.Stream(ctx)
		c, err := ioutil.ReadAll(stream)
		if assert.NoError(t.T(), err) {
			if assert.Len(t.T(), c, len(streamContents)) {
				assert.Equal(t.T(), streamContents, string(c))
			}
		}

		assert.Equal(t.T(), streamInit.MimeType, stream.MimeType)
		assert.Equal(t.T(), streamInit.FfmpegFormat, stream.FfmpegFormat.String())
		assert.Equal(t.T(), streamInit.OffsetMS, stream.StartOffsetMS)
		assert.Equal(t.T(), time.Unix(streamInit.MediaStartTimeUTC, 0), stream.StartTime)
	}
}

func (t *messageTestSuite) TestReadStreamWithMixedPrefixes() {
	ctx := context.Background()
	prefixes := []string{
		"prefix1__",
		"prefix2__",
		"prefix3__",
	}
	config := MessageReaderConfig{
		Topic:     "stream_1",
		Partition: int32(100),
		KeyPrefix: prefixes[1],
	}

	messagec := make(chan messaging.ConsumerMessage, 27)
	var ret <-chan messaging.ConsumerMessage = messagec
	t.messageClient.On("ConsumeWithPartition", mock.Anything, config.Topic, config.Partition).Return(ret)
	t.messageClient.On("Err").Return(make(<-chan error))

	initMsgs := []messages.StreamInit{
		messages.RandomStreamInit(prefixes[0]),
		messages.RandomStreamInit(prefixes[1]),
		messages.RandomStreamInit(prefixes[2]),
	}

	for i := 0; i < 3; i++ {
		msgValue, _ := json.Marshal(initMsgs[i])
		messagec <- messaging.ConsumerMessage{
			Key:   []byte(initMsgs[i].Key()),
			Value: msgValue,
		}
	}

	contents := []string{
		"abcdefghijklmnopqrstuvwxyz",
		"1234567890!@#$%^&*()",
		"i am some other stream",
	}
	buffers := []*bytes.Buffer{
		bytes.NewBufferString(contents[0]),
		bytes.NewBufferString(contents[1]),
		bytes.NewBufferString(contents[2]),
	}

	// mix messages with different prefixes
	for i := 0; i < 26; i++ {
		for i := 0; i < len(buffers); i++ {
			buf := buffers[i]
			if buf == nil {
				continue
			}
			msg := messages.EmptyRawStream(prefixes[i])
			p := make([]byte, 4)
			n, err := buf.Read(p)
			if err == io.EOF {
				msg := messages.RandomStreamEOF(prefixes[i])
				msgValue, _ := json.Marshal(msg)
				messagec <- messaging.ConsumerMessage{
					Key:   []byte(msg.Key()),
					Value: msgValue,
				}
				buffers[i] = nil
			} else {
				messagec <- messaging.ConsumerMessage{
					Key:   []byte(msg.Key()),
					Value: p[:n],
				}
			}
		}
	}

	sr, err := NewMessageStreamReader(config, t.messageClient)
	if assert.NoError(t.T(), err) && assert.NotNil(t.T(), sr) {
		stream := sr.Stream(ctx)
		c, err := ioutil.ReadAll(stream)
		if assert.NoError(t.T(), err) {
			assert.Equal(t.T(), contents[1], string(c))
		}

		assert.Equal(t.T(), initMsgs[1].MimeType, stream.MimeType)
		assert.Equal(t.T(), initMsgs[1].FfmpegFormat, stream.FfmpegFormat.String())
		assert.Equal(t.T(), initMsgs[1].OffsetMS, stream.StartOffsetMS)
		assert.Equal(t.T(), time.Unix(initMsgs[1].MediaStartTimeUTC, 0), stream.StartTime)
	}
}
