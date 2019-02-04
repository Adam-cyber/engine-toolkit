package main

import (
	"os"
	"testing"
	"time"

	"github.com/matryer/is"
)

// TestNewConfig tests the config default values.
// Default values are set in NewConfig.
func TestNewConfig(t *testing.T) {
	is := is.New(t)
	os.Setenv("VERITONE_WEBHOOK_READY", "http://0.0.0.0:8080/readyz")
	os.Setenv("VERITONE_WEBHOOK_PROCESS", "http://0.0.0.0:8080/process")
	os.Setenv("KAFKA_BROKERS", "0.0.0.0:9092,1.1.1.1:9092")
	os.Setenv("KAFKA_INPUT_TOPIC", "input-topic")
	os.Setenv("KAFKA_CONSUMER_GROUP", "consumer-group")
	os.Setenv("END_IF_IDLE_SECS", "60")

	config := NewConfig()
	is.Equal(config.Webhooks.Ready.URL, "http://0.0.0.0:8080/readyz")
	is.Equal(config.Webhooks.Process.URL, "http://0.0.0.0:8080/process")
	is.Equal(len(config.Kafka.Brokers), 2)
	is.Equal(config.Kafka.Brokers[0], "0.0.0.0:9092")
	is.Equal(config.Kafka.Brokers[1], "1.1.1.1:9092")
	is.Equal(config.Kafka.ConsumerGroup, "consumer-group")
	is.Equal(config.Kafka.InputTopic, "input-topic")
	is.Equal(config.EndIfIdleDuration, 1*time.Minute)
	is.Equal(config.Stdout, os.Stdout)
	is.Equal(config.Stderr, os.Stderr)
	is.Equal(config.Subprocess.Arguments, os.Args[1:])
	is.Equal(config.Subprocess.ReadyTimeout, 1*time.Minute)

	// backoff
	is.Equal(config.Webhooks.Backoff.MaxRetries, 10)
	is.Equal(config.Webhooks.Backoff.InitialBackoffDuration, 100*time.Millisecond)
	is.Equal(config.Webhooks.Backoff.MaxBackoffDuration, 1*time.Second)
}
