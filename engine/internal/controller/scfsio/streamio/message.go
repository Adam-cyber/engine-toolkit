package streamio

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	messages "github.com/veritone/edge-messages"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/messaging"
)

const (
	defaultMessageReaderTimeout = time.Minute * 5
	defaultStreamMessageSize    = 1024 * 10
)

var (
	errEmptyTopic     = errors.New("topic cannot be empty")
	errMessageTimeout = errors.New("timed out while waiting for stream message")
	errConsumerClosed = fmt.Errorf("consumer channel closed before receiving %q message", messages.StreamEOFType)
)

type MessageReaderConfig struct {
	Topic     string
	Partition int32
	KeyPrefix string
	Timeout   time.Duration
}

type messageStreamReader struct {
	messageClient messaging.Client
	config        MessageReaderConfig
}

// NewMessageStreamReader initializes and returns a Streamer instance that reads from a message topic
func NewMessageStreamReader(config MessageReaderConfig, messageClient messaging.Client) (Streamer, error) {
	if config.Topic == "" {
		return nil, errEmptyTopic
	}

	if config.Timeout == 0 {
		config.Timeout = defaultMessageReaderTimeout
	}

	return &messageStreamReader{
		messageClient: messageClient,
		config:        config,
	}, nil
}

func (k *messageStreamReader) Stream(ctx context.Context) *Stream {
	readyc := make(chan bool)
	stream := NewStream()

	go func() {
		defer close(readyc)
		defer stream.done()
		defer func() {
			stream.Close()
		}()

		count := 0
		consumeCtx, cancelConsumer := context.WithCancel(ctx)
		messagec := k.messageClient.ConsumeWithPartition(consumeCtx, k.config.Topic, k.config.Partition)

		stream.OnClose(func() error {
			Logger.Printf("[mr:%s:%d] OnClose", k.config.Topic, k.config.Partition)
			cancelConsumer()
			return nil
		})

		timeout := time.NewTimer(k.config.Timeout)
		defer timeout.Stop()

		for {
			select {
			case <-ctx.Done():
				stream.SendErr(ctx.Err())
				return
			case <-timeout.C:
				stream.SendErr(errMessageTimeout)
				return
			case err := <-k.messageClient.Err():
				if err != nil {
					stream.SendErr(fmt.Errorf("consumer error: %s", err))
					return
				}
			case msg, ok := <-messagec:
				if !ok {
					stream.SendErr(errConsumerClosed)
					return
				}

				// skip messages whose keys do not have the configured prefix
				if !strings.HasPrefix(string(msg.Key), k.config.KeyPrefix) {
					continue
				}

				count++
				timeout.Reset(k.config.Timeout)
				key := strings.TrimPrefix(string(msg.Key), k.config.KeyPrefix)

				if key == string(messages.StreamEOFType) {
					Logger.Println("detected end of stream")
					return
				}

				if count == 1 {
					if key != string(messages.StreamInitType) {
						err := fmt.Errorf("expected first message on stream topic to be of type %s, got %q", messages.StreamInitType, msg.Key)
						stream.SendErr(err)
						return
					}

					// get mime type and relative time offset from stream init message
					var initMsg messages.StreamInit
					if err := json.Unmarshal(msg.Value, &initMsg); err != nil {
						stream.SendErr(fmt.Errorf("invalid %s message value: %s", messages.StreamInitType, err))
						return
					}

					Logger.Println("received init message:", string(msg.Value))

					stream.StartOffsetMS = initMsg.OffsetMS
					stream.MimeType = initMsg.MimeType

					if initMsg.FfmpegFormat != "" {
						stream.SetFfmpegFormat(initMsg.FfmpegFormat)
					}

					if initMsg.MediaStartTimeUTC > 0 {
						stream.StartTime = time.Unix(initMsg.MediaStartTimeUTC, 0)
						if y := stream.StartTime.Year(); y < 0 || y > 9999 {
							stream.SendErr(fmt.Errorf(`invalid message value for "mediaStartTimeUTC": %d`, initMsg.MediaStartTimeUTC))
							return
						}
					}

					readyc <- true
					continue
				}

				_, err := stream.Write(msg.Value)
				if err != nil {
					stream.SendErr(fmt.Errorf("write failed: %s", err))
					return
				}
			}
		}
	}()

	<-readyc
	return stream
}

type messageWriter struct {
	config       MessageStreamWriterConfig
	streamClient *messaging.Helper
	bytesWritten *Progress
}

// MessageStreamWriterConfig contains the options for configuring a message stream writer instance
type MessageStreamWriterConfig struct {
	Topic          string
	Partition      int32
	KeyPrefix      string
	MaxMessageSize int
	NoDataTimeout  time.Duration
}

func (c *MessageStreamWriterConfig) defaults() {
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = defaultStreamMessageSize
	}
	if c.NoDataTimeout == 0 {
		c.NoDataTimeout = defaultNoDataTimeout
	}
}

// NewMessageStreamWriter initializes and returns a new StreamWriter instance
func NewMessageStreamWriter(streamClient *messaging.Helper, config MessageStreamWriterConfig) StreamWriter {
	config.defaults()
	return &messageWriter{
		config:       config,
		streamClient: streamClient,
		bytesWritten: new(Progress),
	}
}

func (s *messageWriter) BytesWritten() int64 {
	return s.bytesWritten.Count()
}

func (s *messageWriter) WriteStream(ctx context.Context, stream *Stream) error {
	md5Hash := md5.New()
	method := fmt.Sprintf("[mw:%s:%d] WriteStream", s.config.Topic, s.config.Partition)
	defer func() {
		// send stream EOF message
		Logger.Println(method, "Sending stream EOF")
		eofMsg := messages.EmptyStreamEOF(s.config.KeyPrefix)
		eofMsg.ContentLength = s.bytesWritten.Count()
		eofMsg.MD5Checksum = hex.EncodeToString(md5Hash.Sum(nil))

		err := s.streamClient.ProduceStreamEOFMessage(ctx, s.config.Topic, s.config.Partition, &eofMsg)
		if err != nil {
			Logger.Println(method, "Failed to produce stream EOF message: ", err)
		}
	}()

	// send stream_init message
	initMsg := messages.EmptyStreamInit(s.config.KeyPrefix)
	initMsg.OffsetMS = stream.StartOffsetMS
	initMsg.MediaStartTimeUTC = stream.StartTime.Unix()
	initMsg.MimeType = stream.MimeType
	initMsg.FfmpegFormat = stream.FfmpegFormat.String()

	Logger.Println(method, "Sending stream init message:")

	err := s.streamClient.ProduceStreamInitMessage(ctx, s.config.Topic, s.config.Partition, &initMsg)
	if err != nil {
		return fmt.Errorf("%s failed to write stream init message: %s", method, err)
	}

	Logger.Printf("%s Init message: %+#v", method, initMsg)

	errc := make(chan error)
	writeTimeout := time.NewTimer(s.config.NoDataTimeout)
	defer writeTimeout.Stop()

	go func() {
		scanner := bufio.NewScanner(stream)
		scanner.Split(genSplitFunc(s.config.MaxMessageSize))

		for {
			if !scanner.Scan() {
				errc <- scanner.Err()
				return
			}

			// if a token was scanned, write it as a raw stream message
			if chunk := scanner.Bytes(); len(chunk) > 0 {
				msg := messages.EmptyRawStream(s.config.KeyPrefix)
				msg.Value = chunk
				err := s.streamClient.ProduceRawStreamMessage(ctx, s.config.Topic, s.config.Partition, &msg)
				if err != nil {
					errc <- fmt.Errorf("failed to write stream chunk: %s", err)
					return
				}

				s.bytesWritten.Write(chunk)
				md5Hash.Write(chunk)
				writeTimeout.Reset(s.config.NoDataTimeout)
			}
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-writeTimeout.C:
		return ErrWriterNoDataTimeout

	case err := <-errc:
		return err
	}
}

func genSplitFunc(numBytes int) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if len(data) >= numBytes {
			return numBytes, data[0:numBytes], nil
		}
		// If we're at EOF, we have a final token with less than numBytes bytes. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
}
