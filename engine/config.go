package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds engine configuration settings.
type Config struct {
	// Engine contains engine specific configuration and information.
	Engine struct {
		// ID is the identifier for this engine.
		ID string
		// InstanceID is the instance ID for this running instance.
		InstanceID string
		// EndIfIdleDuration is the duration after the last message
		// at which point the engine will shut down.
		EndIfIdleDuration time.Duration
	}
	// Processing contains configuration about how the engine toolkit
	// handles work.
	Processing struct {
		// Concurrency is the number of tasks to run concurrently.
		Concurrency int
		// DisableChunkDownload disables the automatic download of the chunk.
		DisableChunkDownload bool
	}
	// Stdout is the Engine's stdout. Subprocesses inherit this.
	Stdout io.Writer
	// Stderr is the Engine's stderr. Subprocesses inherit this.
	Stderr io.Writer
	// Subprocess holds configuration relating to the subprocess
	// that this engine supervises.
	Subprocess struct {
		// Arguments are the command line arguments (including the command as the
		// first argument) for the subprocess.
		// By default, these are taken from the arguments passed to this tool.
		Arguments []string
		// ReadyTimeout is the amount of time to wait before deciding that the subprocess
		// is not going to be ready.
		ReadyTimeout time.Duration
	}
	// Kafka holds Kafka configuration.
	Kafka struct {
		// Brokers is a list of Kafka brokers.
		Brokers []string
		// ConsumerGroup is the group name of the consumers.
		ConsumerGroup string
		// InputTopic is the topic on which chunks are received.
		InputTopic string
		// ChunkTopic is the output topic where chunk results are sent.
		ChunkTopic string
		// EventTopic is the topic to push events like metrics and timing information
		EventTopic string
	}
	// Webhooks holds webhook addresses.
	Webhooks struct {
		// Ready holds configuration for the readiness webhook.
		Ready struct {
			// URL is the address of the Readiness Webhook.
			URL string
			// PollDuration is how often the URL will be polled to check
			// for readiness before processing begins.
			PollDuration time.Duration
			// MaximumPollDuration is the maximum of time to allow for the
			// engine to become ready before abandoning the operation altogether.
			MaximumPollDuration time.Duration
		}
		// Process holds configuration for the processing webhook.
		Process struct {
			// URL is the address of the Processing Webhook.
			URL string
		}
		// Backoff controls webhook backoff and retry policy.
		Backoff struct {
			// MaxRetries is the maximum number of retries that will be made before
			// giving up.
			MaxRetries int
			// InitialBackoffDuration is the time to wait before the first retry.
			InitialBackoffDuration time.Duration
			// MaxBackoffDuration is the maximum amount of time to wait before retrying.
			MaxBackoffDuration time.Duration
		}
	}
	// Events contains system event configuration.
	Events struct {
		// PeriodicUpdateDuration is the interval at which to
		// send periodic updates during processing.
		PeriodicUpdateDuration time.Duration
	}
	// SelfDriving contains configuration related to running the engine in
	// self driving mode, drawing input from a directory and writing output to another.
	SelfDriving struct {
		// SelfDrivingMode is whether this engine is in self driving mode or not.
		SelfDrivingMode bool
		// InputPattern is the filepath.Glob pattern to use when selecting input files.
		InputPattern string
		// PollInterval is the time to wait before checking for input files.
		PollInterval time.Duration
		// MinimumModifiedDuration is the amount of time to wait after
		// a file is modified before considering it for selection.
		// This is used to protect against files that take a long time to
		// finish copying.
		MinimumModifiedDuration time.Duration
		// WaitForReadyFiles will wait for input files to have a `*.ready` marker file
		// before it will be processed.
		WaitForReadyFiles bool
	}
}

// NewConfig gets default configuration settings.
func NewConfig() Config {
	var c Config

	c.Subprocess.Arguments = os.Args[1:]
	c.Subprocess.ReadyTimeout = 1 * time.Minute
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr

	c.Processing.Concurrency = 1
	if concurrencyStr := os.Getenv("VERITONE_CONCURRENT_TASKS"); concurrencyStr != "" {
		// try and set concurrency (bad values will be logged and ignored)
		var err error
		if c.Processing.Concurrency, err = strconv.Atoi(concurrencyStr); err != nil {
			fmt.Fprintf(os.Stderr, "malformed VERITONE_CONCURRENT_TASKS (expected int) reverting to 1: %s\n", concurrencyStr)
			c.Processing.Concurrency = 1
		}
	}

	c.Engine.InstanceID = os.Getenv("ENGINE_INSTANCE_ID")
	c.Engine.ID = os.Getenv("ENGINE_ID")
	c.Processing.DisableChunkDownload = os.Getenv("VERITONE_DISABLE_CHUNK_DOWNLOAD") == "true"

	c.Webhooks.Ready.URL = os.Getenv("VERITONE_WEBHOOK_READY")
	c.Webhooks.Ready.PollDuration = 1 * time.Second
	c.Webhooks.Ready.MaximumPollDuration = 1 * time.Minute
	c.Webhooks.Process.URL = os.Getenv("VERITONE_WEBHOOK_PROCESS")
	c.Webhooks.Backoff.MaxRetries = 3
	c.Webhooks.Backoff.InitialBackoffDuration = 100 * time.Millisecond
	c.Webhooks.Backoff.MaxBackoffDuration = 1 * time.Second

	// veritone platform configuration
	if endSecs := os.Getenv("END_IF_IDLE_SECS"); endSecs != "" {
		var err error
		c.Engine.EndIfIdleDuration, err = time.ParseDuration(endSecs + "s")
		if err != nil {
			log.Printf("END_IF_IDLE_SECS %q: %v", endSecs, err)
		}
	}
	if c.Engine.EndIfIdleDuration == 0 {
		c.Engine.EndIfIdleDuration = 1 * time.Minute
	}
	// kafka configuration
	c.Kafka.Brokers = strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	c.Kafka.ConsumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP")
	c.Kafka.InputTopic = os.Getenv("KAFKA_INPUT_TOPIC")
	c.Kafka.ChunkTopic = os.Getenv("KAFKA_CHUNK_TOPIC")

	// fixed parameters for event info
	c.Kafka.EventTopic = "events"
	c.Events.PeriodicUpdateDuration = 1 * time.Minute

	// self driving mode
	c.SelfDriving.SelfDrivingMode = os.Getenv("VERITONE_SELFDRIVING") == "true"
	c.SelfDriving.WaitForReadyFiles = os.Getenv("VERITONE_SELFDRIVING_WAITREADYFILES") == "true"
	c.SelfDriving.InputPattern = os.Getenv("VERITONE_SELFDRIVING_INPUTPATTERN")
	if interval := os.Getenv("VERITONE_SELFDRIVING_POLLINTERVAL"); interval != "" {
		var err error
		c.SelfDriving.PollInterval, err = time.ParseDuration(interval)
		if err != nil {
			log.Printf("VERITONE_SELFDRIVING_POLLINTERVAL %q: %v", interval, err)
		}
	}
	if interval := os.Getenv("VERITONE_SELFDRIVING_MINIMUM_MODIFIED_DURATION"); interval != "" {
		var err error
		c.SelfDriving.MinimumModifiedDuration, err = time.ParseDuration(interval)
		if err != nil {
			log.Printf("VERITONE_SELFDRIVING_MINIMUM_MODIFIED_DURATION %q: %v", interval, err)
		}
	}

	return c
}
