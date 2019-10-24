package processing

import "time"

// Engine contains engine specific configuration and information.
type Engine struct {
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
type Processing struct {
	// Concurrency is the number of tasks to run concurrently.
	Concurrency int
	// DisableChunkDownload disables the automatic download of the chunk.
	DisableChunkDownload bool
}

// Subprocess holds configuration relating to the subprocess
// that this engine supervises.
type Subprocess struct {
	// Arguments are the command line arguments (including the command as the
	// first argument) for the subprocess.
	// By default, these are taken from the arguments passed to this tool.
	Arguments []string
	// ReadyTimeout is the amount of time to wait before deciding that the subprocess
	// is not going to be ready.
	ReadyTimeout time.Duration
}

// Kafka holds Kafka configuration.
type Kafka struct {
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
type Webhooks struct {
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
