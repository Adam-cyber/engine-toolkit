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
	os.Setenv("END_AFTER_SECS", "60")
	os.Setenv("VERITONE_CONCURRENT_TASKS", "10")

	os.Setenv("ENGINE_INSTANCE_ID", "instance1")
	os.Setenv("ENGINE_ID", "engine1")
	os.Setenv("VERITONE_SELFDRIVING", "true")
	defer os.Setenv("VERITONE_SELFDRIVING", "false")
	os.Setenv("VERITONE_SELFDRIVING_POLLINTERVAL", "5m")
	os.Setenv("VERITONE_SELFDRIVING_MINIMUM_MODIFIED_DURATION", "5m")
	os.Setenv("VERITONE_SELFDRIVING_INPUTPATTERN", "*.jpg")
	os.Setenv("VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN", "yyyy/mm/dd")
	os.Setenv("VERITONE_SELFDRIVING_WAITREADYFILES", "true")
	os.Setenv("VERITONE_DISABLE_CHUNK_DOWNLOAD", "true")

	config := NewConfig()
	is.Equal(config.Processing.DisableChunkDownload, true)
	is.Equal(config.Webhooks.Ready.URL, "http://0.0.0.0:8080/readyz")
	is.Equal(config.Webhooks.Process.URL, "http://0.0.0.0:8080/process")
	is.Equal(len(config.Kafka.Brokers), 2)
	is.Equal(config.Kafka.Brokers[0], "0.0.0.0:9092")
	is.Equal(config.Kafka.Brokers[1], "1.1.1.1:9092")
	is.Equal(config.Kafka.ConsumerGroup, "consumer-group")
	is.Equal(config.Kafka.InputTopic, "input-topic")
	is.Equal(config.Kafka.EventTopic, "events")

	is.Equal(config.Engine.ID, "engine1")
	is.Equal(config.Engine.InstanceID, "instance1")
	is.Equal(config.Engine.EndIfIdleDuration, 1*time.Minute)
	is.Equal(config.Engine.EndAfterDuration, 1*time.Minute)
	is.Equal(config.Processing.Concurrency, 10)
	is.Equal(config.Stdout, os.Stdout)
	is.Equal(config.Stderr, os.Stderr)
	is.Equal(config.Subprocess.Arguments, os.Args[1:])
	is.Equal(config.Subprocess.ReadyTimeout, 1*time.Minute)

	// backoff
	is.Equal(config.Webhooks.Backoff.MaxRetries, 3)
	is.Equal(config.Webhooks.Backoff.InitialBackoffDuration, 100*time.Millisecond)
	is.Equal(config.Webhooks.Backoff.MaxBackoffDuration, 1*time.Second)

	// events
	is.Equal(config.Events.PeriodicUpdateDuration, 1*time.Minute)

	// self driving
	is.Equal(config.SelfDriving.SelfDrivingMode, true)
	is.Equal(config.SelfDriving.PollInterval, 5*time.Minute)
	is.Equal(config.SelfDriving.MinimumModifiedDuration, 5*time.Minute)
	is.Equal(config.SelfDriving.WaitForReadyFiles, true)
	is.Equal(config.SelfDriving.InputPattern, "*.jpg")
	is.Equal(config.SelfDriving.OutputDirPattern, "yyyy/mm/dd")

}
