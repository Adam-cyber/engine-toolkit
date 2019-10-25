package main

import (
	"fmt"
	"github.com/veritone/engine-toolkit/engine/internal/controller"
	"github.com/veritone/engine-toolkit/engine/processing"
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
	Engine processing.Engine
	// Processing contains configuration about how the engine toolkit
	// handles work.
	Processing processing.Processing
	// Stdout is the Engine's stdout. Subprocesses inherit this.
	Stdout io.Writer
	// Stderr is the Engine's stderr. Subprocesses inherit this.
	Stderr io.Writer
	// Subprocess holds configuration relating to the subprocess
	// that this engine supervises.
	Subprocess processing.Subprocess
	// Kafka holds Kafka configuration.
	Kafka processing.Kafka
	// Webhooks holds webhook addresses.
	Webhooks processing.Webhooks
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
		// OutputDirPattern is the output folder pattern. This path will be appended to each
		// of the other output paths during processing. OutputDirPattern supports some time
		// tokens (such as "yyyy" for the current year).
		OutputDirPattern string
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
	ControllerConfig controller.VeritoneControllerConfig
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
	c.SelfDriving.OutputDirPattern = os.Getenv("VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN")
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

	// controller mode
	/** TODO pick up from realtime/config .. for now we'll just pick up a few
	controllerConfig := os.Getenv("VERITONE_CONTROLLER_CONFIG_JSON")
	if controllerConfig != "" {
		// deserialize it
		err := json.Unmarshal([]byte(controllerConfig), &c.ControllerConfig)
		if err == nil {
			c.ControllerConfig.Kafka = c.Kafka   // propagate the config here..
			c.ControllerConfig.Webhooks = c.Webhooks
			c.ControllerConfig.SetDefaults()
		} else {
			log.Printf("Unable to unmarshal VERITONE_CONTROLLER_CONFIG_JSON, err=%v", err)
			c.ControllerConfig.ControllerMode = false
		}
	}
	*/
	if os.Getenv("AIWARE_CONTROLLER") != "" {
		// trigger on controller
		c.ControllerConfig.ControllerMode = true
		c.ControllerConfig.HostId = os.Getenv("AIWARE_HOST_ID")
		c.ControllerConfig.ControllerUrl = os.Getenv("AIWARE_CONTROLLER")
		c.ControllerConfig.LaunchId = os.Getenv("AIWARE_LAUNCH_ID")
		c.ControllerConfig.Kafka = c.Kafka
		// with the exception of kafka brokers we want to pick up AIWARE_KAFKA_BROKERS

		c.ControllerConfig.Kafka.Brokers = strings.Split(os.Getenv("AIWARE_KAFKA_BROKERS"), ",")

		c.ControllerConfig.Webhooks = c.Webhooks
		c.ControllerConfig.ProcessingOptions = c.Processing
		c.ControllerConfig.SetDefaults()
	}
	return c
}
