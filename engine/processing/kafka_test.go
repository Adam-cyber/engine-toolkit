package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/matryer/is"
	"github.com/pkg/errors"
)

func TestMediaChunkMessagePayload(t *testing.T) {
	is := is.New(t)

	var m MediaChunkMessage
	src := `{
		"jobId": "JobID",
		"taskPayload": {
			"token": "abc123"
		}
	}`
	is.NoErr(json.Unmarshal([]byte(src), &m))
	is.True(strings.Contains(strings.TrimSpace(string(m.TaskPayload)), `"token": "abc123"`))

	payload, err := m.unmarshalPayload()
	is.NoErr(err)
	is.Equal(payload.Token, "abc123")
}

func TestKafkaIntegration(t *testing.T) {
	if os.Getenv("KAFKATEST") == "" {
		t.Skip("skipping unless KAFKATEST=true")
	}
	is := is.New(t)
	consumerGroup := "test-group-2"
	topic := fmt.Sprintf("%v", time.Now().Unix())
	consumer, cleanup, err := newKafkaConsumer([]string{"localhost:9092"}, consumerGroup, topic)
	is.NoErr(err)
	defer cleanup()
	defer consumer.Close()
	var wg sync.WaitGroup
	go func() {
		for msg := range consumer.Messages() {
			is.Equal(string(msg.Key), "test-key")
			is.Equal(string(msg.Value), "test-value")
			wg.Done()
		}
	}()
	time.Sleep(1 * time.Second)
	producer, err := newKafkaProducer([]string{"localhost:9092"})
	is.NoErr(err)
	defer producer.Close()
	wg.Add(1)
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	})
	is.NoErr(err)
	wg.Wait()
}

func TestPipe(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	pipe := newPipe()
	defer pipe.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
		case msg := <-pipe.Messages():
			is.Equal(string(msg.Key), "test-key")
			is.Equal(string(msg.Value), "test-value")
		}
	}()

	_, _, err := pipe.SendMessage(&sarama.ProducerMessage{
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	})
	is.NoErr(err)

	wg.Wait() // wait for the message to be received
	is.NoErr(ctx.Err())
}

// newPipe makes a new pipe that acts like an in-memory Kafka
// implementation. Used for testing.
func newPipe() *pipe {
	p := &pipe{
		ch: make(chan *sarama.ConsumerMessage, 10),
	}
	return p
}

type pipe struct {
	ch     chan *sarama.ConsumerMessage
	Offset int64
}

func (p *pipe) Close() error {
	close(p.ch)
	return nil
}

func (p *pipe) Messages() <-chan *sarama.ConsumerMessage {
	return p.ch
}

func (p *pipe) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if msg.Key == nil {
		return 0, 0, errors.New("missing Key")
	}
	key, err := msg.Key.Encode()
	if err != nil {
		return 0, 0, errors.Wrap(err, "key encode")
	}
	value, err := msg.Value.Encode()
	if err != nil {
		return 0, 0, errors.Wrap(err, "value encode")
	}
	p.ch <- &sarama.ConsumerMessage{
		Topic:     msg.Topic,
		Key:       key,
		Value:     value,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}
	return 0, 0, nil
}

func (p *pipe) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	p.Offset = msg.Offset
}
