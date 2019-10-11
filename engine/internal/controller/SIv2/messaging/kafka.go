package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pborman/uuid"
)

const defaultMaxProcessingTime = time.Second

type kafkaClient struct {
	producerRandom sarama.SyncProducer
	producerManual sarama.SyncProducer
	producerHash   sarama.SyncProducer
	consumer       sarama.Consumer
	errc           chan error
	debug          bool
}

type KafkaClientConfig struct {
	Brokers            []string `json:"brokers,omitempty"`
	ClientIDPrefix     string   `json:"clientIdPrefix,omitempty"`
	MaxProcessingTime  string   `json:"maxProcessingTime,omitempty"`
	ProducerTimeout    string   `json:"producerTimeout,omitempty"`
	ProducerMaxRetries int      `json:"producerMaxRetries,omitempty"`
	Debug              bool     `json:"debug"`
}

func NewKafkaClient(config KafkaClientConfig) (Client, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("must specify at least one kafka broker")
	}
	if config.Debug {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.LstdFlags)
	}

	// setup producer - produces across partitions
	cfg := sarama.NewConfig()
	cfg.ClientID = config.ClientIDPrefix + uuid.New()
	// VTN-27185: Change Partitioner from NewRoundRobinPartitioner to NewRandomPartitioner
	// When a producer is created with Partitioner type is RoundRobin, Kafka messages are always started from partition 0.
	// It is the root cause why in PMI cluster we see only lag on 4 partitions (0, 1, 2, 3). Sometimes the lag is very high (VTN-23992).
	// We need to update Partitioner type to Random for better the system.
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	// cfg.Producer.Compression = sarama.CompressionSnappy // enable for message compression
	cfg.Producer.Return.Successes = true // enabled for sync producer
	cfg.Producer.Return.Errors = true    // enabled for sync producer
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 1 * time.Second
	cfg.Metadata.Retry.Max = 5
	cfg.Metadata.Retry.Backoff = 1 * time.Second
	if config.ProducerMaxRetries > 0 {
		cfg.Producer.Retry.Max = config.ProducerMaxRetries
	}
	if config.ProducerTimeout != "" {
		var err error
		cfg.Producer.Timeout, err = time.ParseDuration(config.ProducerTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid producer timeout value %q: %s", config.ProducerTimeout, err)
		}
	}
	producerRandom, err := sarama.NewSyncProducer(config.Brokers, cfg)
	if err != nil {
		return nil, err
	}
	cfgCopy0 := *cfg
	cfgCopy0.Producer.Partitioner = sarama.NewHashPartitioner
	producerHash, err := sarama.NewSyncProducer(config.Brokers, &cfgCopy0)
	if err != nil {
		return nil, err
	}

	// this producer produces to a single manually-specified partition
	cfgCopy1 := *cfg
	cfgCopy1.Producer.Partitioner = sarama.NewManualPartitioner
	parititonProducer, err := sarama.NewSyncProducer(config.Brokers, &cfgCopy1)
	if err != nil {
		return nil, err
	}

	// setup consumer
	cfg = sarama.NewConfig()
	cfg.ClientID = config.ClientIDPrefix + uuid.New()
	cfg.Consumer.MaxProcessingTime = defaultMaxProcessingTime
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Retry.Backoff = 1 * time.Second
	cfg.Consumer.Offsets.Retry.Max = 5
	cfg.Metadata.Retry.Max = 5
	cfg.Metadata.Retry.Backoff = 1 * time.Second
	if config.MaxProcessingTime != "" {
		cfg.Consumer.MaxProcessingTime, _ = time.ParseDuration(config.MaxProcessingTime)
	}
	consumer, err := sarama.NewConsumer(config.Brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &kafkaClient{
		producerRandom: producerRandom,
		producerHash:   producerHash,
		producerManual: parititonProducer,
		consumer:       consumer,
		errc:           make(chan error, 1),
		debug:          config.Debug,
	}, nil
}

func (k *kafkaClient) ProduceRandom(ctx context.Context, topic string, key string, value []byte) (error, int32, int64) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	return k.produce(ctx, k.producerRandom, msg)
}

func (k *kafkaClient) ProduceHash(ctx context.Context, topic string, key string, value []byte) (error, int32, int64) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	return k.produce(ctx, k.producerHash, msg)
}

func (k *kafkaClient) ProduceManual(ctx context.Context, topic string, partition int32, key string, value []byte) (error, int32, int64) {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(value),
	}
	return k.produce(ctx, k.producerManual, msg)
}

func (k *kafkaClient) Consume(ctx context.Context, topic string) <-chan ConsumerMessage {
	return k.consume(ctx, topic, 0)
}

func (k *kafkaClient) ConsumeWithPartition(ctx context.Context, topic string, partition int32) <-chan ConsumerMessage {
	// by default consume from first partition
	if partition < 0 {
		partition = 0
	}
	return k.consume(ctx, topic, partition)
}

func (k *kafkaClient) consume(ctx context.Context, topic string, partition int32) <-chan ConsumerMessage {
	messagec := make(chan ConsumerMessage, 1)
	go func() {
		consumer, err := k.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			k.errc <- err
			return
		}
		defer consumer.Close()
		for {
			select {
			case err := <-consumer.Errors():
				k.errc <- err
				return
			case msg := <-consumer.Messages():
				if msg != nil {
					messagec <- ConsumerMessage(*msg)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return messagec
}

func (k *kafkaClient) Err() <-chan error {
	return k.errc
}

func (k *kafkaClient) Close() []error {
	var errs []error
	if err := k.producerHash.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := k.producerManual.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := k.producerRandom.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := k.consumer.Close(); err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (k *kafkaClient) logf(format string, v ...interface{}) {
	if k.debug {
		log.Printf(format, v...)
	}
}

func (k *kafkaClient) produce(ctx context.Context, producer sarama.SyncProducer, msg *sarama.ProducerMessage) (error, int32, int64) {
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err, 0, 0
	}
	k.logf("Message stored in topic(%s)/partition(%d)/offset(%d)/bytes(%d)", msg.Topic, partition, offset, msg.Value.Length())
	return nil, partition, offset
}
