package adapter

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
	util "github.com/veritone/realtime/modules/engines/scfsio"
	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter/api"
	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter/messaging"
)

const defaultHeartbeatInterval = "5s"
const defaultChunkSize = 512 * 1024 //512K

var (
	/* TODO REMOVEME
	payloadFlag = flag.String("payload", "", "payload file")
	configFlag  = flag.String("config", "", "config file")

	tdoIDFlag   = flag.String("tdo", "", "Temporal data object ID (override payload)")
	urlFlag     = flag.String("url", "", "Override URL to ingest")
	stdoutFlag  = flag.Bool("stdout", false, "Write stream to stdout instead of Kafka")
		*/
)

func init() {
	flag.Parse()
}

type engineConfig struct {
	EngineID               string                 `json:"engineId,omitempty"`
	EngineInstanceID       string                 `json:"engineInstanceId,omitempty"`
	VeritoneAPI            api.Config             `json:"veritoneAPI,omitempty"`
	Messaging              messaging.ClientConfig `json:"messaging,omitempty"`
	FFMPEG                 ffmpegConfig           `json:"ffmpeg,omitempty"`
	HeartbeatInterval      string                 `json:"heartbeatInterval,omitempty"`
	OutputTopicName        string                 `json:"outputTopicName,omitempty"`
	OutputTopicPartition   int32                  `json:"outputTopicPartition"`
	OutputTopicKeyPrefix   string                 `json:"outputTopicKeyPrefix"`
	OutputBucketName       string                 `json:"outputBucketName,omitempty"`
	OutputBucketRegion     string                 `json:"outputBucketRegion,omitempty"`
	MinioServer            string                 `json:"minioServer,omitempty"`
	SupportedTextMimeTypes []string               `json:"supportedTextMimeTypes,omitempty"`
}

func (c *engineConfig) String() string {
	j, _ := json.Marshal(c)
	return string(j)
}

func (c *engineConfig) validate() error {
	if _, err := time.ParseDuration(c.HeartbeatInterval); err != nil {
		return fmt.Errorf(`invalid value for "heartbeatInterval": "%s" - %s`, c.HeartbeatInterval, err)
	}
	/* TODO REMOVEME
	if (stdoutFlag == nil || !*stdoutFlag) && c.OutputTopicName == "" && c.OutputBucketName == "" {
		return errors.New("an output topic or bucket name is required")
	}
	*/
	return nil
}

func (c *engineConfig) defaults() {
	if c.HeartbeatInterval == "" {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}

	// Stuff in defaults to avoid loading the built config.json, since we may not be guaranteed that we wuld have files
	c.Messaging.Kafka.ProducerMaxRetries = 1000
	c.Messaging.Kafka.ProducerTimeout = "10s"

	c.VeritoneAPI.Timeout = "60s"
	c.VeritoneAPI.GraphQLEndpoint = "/v3/graphql"
}

type taskPayload struct {
	RecordStartTime      time.Time          `json:"recordStartTime,omitempty"`
	RecordEndTime        time.Time          `json:"recordEndTime,omitempty"`
	RecordDuration       string             `json:"recordDuration,omitempty"`
	StartTimeOverride    int64              `json:"startTimeOverride,omitempty"`
	TDOOffsetMS          int                `json:"tdoOffsetMS,omitempty"`
	SourceID             string             `json:"sourceId,omitempty"`
	SourceDetails        *api.SourceDetails `json:"sourceDetails,omitempty"`
	ScheduledJobID       string             `json:"scheduledJobId,omitempty"`
	URL                  string             `json:"url,omitempty"`
	CacheToS3Key         string             `json:"cacheToS3Key,omitempty"`
	DisableKafka         bool               `json:"disableKafka,omitempty"`
	DisableS3            bool               `json:"disableS3,omitempty"`
	ScfsWriterBufferSize int                `json:"scfsWriterBufferSize,omitempty"`
	OrganizationID       int64              `json:"organizationId,omitempty"`

	// Optional here in the payload to see if this will speed up anything.. default is like 10K?
	ChunkSize int64 `json:"chunkSize,omitempty"`
}

type enginePayload struct {
	JobID              string `json:"jobId,omitempty"`
	TaskID             string `json:"taskId,omitempty"`
	TDOID              string `json:"recordingId,omitempty"`
	Token              string `json:"token,omitempty"`
	VeritoneApiBaseUrl string `json:"veritoneApiBaseUrl"`
	taskPayload        `json:"taskPayload,omitempty"`
}

/** Helper function to display adapter sample payload and env variables thatSampleStreamIngestorV2Payloadenv the adapter needs */
func SampleAdapterPayload () string {
	now := time.Now()
	sampleTaskPayload:=taskPayload{
		RecordStartTime: now.Add(-(5*time.Minute)) ,
		RecordEndTime: now.Add(5 * time.Minute),
		RecordDuration : "10m",
		StartTimeOverride: time.Now().Unix(),
		TDOOffsetMS: 50,
		SourceID: "sourceId",
		SourceDetails:&api.SourceDetails{
			URL: "mySourceDetailsURL",
			RadioStreamURL:"mySourceDetailsRadioStreamURL",
		},
		URL : "myURL",

		CacheToS3Key: "myCacheToS3Key",
		DisableKafka: false,
		DisableS3: false,
		ScfsWriterBufferSize: 50000,
		OrganizationID: 9999,
		ChunkSize: 1024*1000,

	}
	payload := enginePayload{
		JobID:"myJobId",
		TaskID:"myTaskId",
		TDOID: "myTDOID",
		Token: "myToken",
		VeritoneApiBaseUrl: "https://api.veritone.com",
		taskPayload:sampleTaskPayload,
	}

	type envvar struct {
		Name string `json:"name"`
		Required bool `json:"required"`
		DefaultValue string `json:"defaultValue"`
	}
	envVariables:=[]envvar {
		{ Name:"KAFKA_BROKERS", Required:true},
		{ Name: "KAFKA_ENGINE_STATUS_TOPIC",DefaultValue:"engine_status"},
		{ Name: "CHUNK_CACHE_BUCKET", Required:false},
		{ Name: "MINIO_SERVER", Required:false},
		{ Name: "CHUNK_CACHE_AWS_REGION",Required:false},
		{Name: "VERITONE_API_TOKEN", Required:false},}
	return fmt.Sprintf("SampleAdapterPayload:\n%s\nENV Variables: %s\n", util.ToString(payload), util.ToString(envVariables))
}

func (p enginePayload) String() string {
	// redact Token field so it doesn't leak into log
	p.Token = ""
	j, _ := json.Marshal(p)
	return string(j)
}

func (p *enginePayload) defaults() {
	/*
	if tdoIDFlag != nil && *tdoIDFlag != "" {
		p.TDOID = *tdoIDFlag
	}
	if urlFlag != nil && *urlFlag != "" {
		p.URL = *urlFlag
	}*/
	if p.ChunkSize == 0 {
		p.ChunkSize = defaultChunkSize
	}
}

func (p *enginePayload) isOfflineMode() bool {
	return p.CacheToS3Key != ""
}

func (p *enginePayload) isTimeBased() bool {
	return !p.RecordStartTime.IsZero() || !p.RecordEndTime.IsZero() || p.RecordDuration != ""
}

func (p *enginePayload) validate() error {
	if p.isOfflineMode() {
		if p.SourceID == "" {
			return errors.New("missing sourceID")
		}
		if p.ScheduledJobID == "" {
			return errors.New("missing scheduledJobId")
		}
		if p.SourceDetails == nil {
			return errors.New("missing sourceDetails")
		}
	} else {
		if p.TDOID == "" {
			return errors.New("missing tdoId")
		}
		if p.SourceID == "" && p.URL == "" {
			return errors.New("either sourceId or URL is required")
		}
	}

	if p.JobID == "" {
		return errors.New("missing jobId")
	}
	if p.TaskID == "" {
		return errors.New("missing taskId")
	}

	if p.RecordDuration != "" {
		if !p.RecordStartTime.IsZero() || !p.RecordEndTime.IsZero() {
			return errors.New(`only one of "recordDuration" or "recordStartTime"/"recordEndTime" should be provided`)
		}
		if _, err := time.ParseDuration(p.RecordDuration); err != nil {
			return fmt.Errorf(`invalid record duration given "%s": %s`, p.RecordDuration, err)
		}
	}

	if p.RecordEndTime.IsZero() && !p.RecordStartTime.IsZero() {
		return errors.New(`"recordEndTime" is required when "recordStartTime" is set`)
	}

	return nil
}

func loadPayloadFromFile(p interface{}, payloadFilePath string) error {
	if payloadFilePath == "" {
		return errors.New("no payload file specified")
	}

	reader, err := os.Open(payloadFilePath)
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(reader).Decode(p)
}

func loadConfigAndPayload(payloadJSON string, engineID, engineInstanceID string, graphQlTimeoutDuration string) (*engineConfig, *enginePayload, error) {
	payload, config := new(enginePayload), new(engineConfig)

	if err := json.Unmarshal([]byte(payloadJSON), payload); err != nil {
		return config, payload, fmt.Errorf("failed to unmarshal payload JSON: %s", err)
	}

	payload.defaults()
	if err := payload.validate(); err != nil {
		return config, payload, fmt.Errorf("invalid payload: %s", err)
	}

	config.defaults()

	config.EngineID = engineID
	config.EngineInstanceID = engineInstanceID

	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); len(kafkaBrokers) > 0 {
		config.Messaging.Kafka.Brokers = strings.Split(kafkaBrokers, ",")
	}
	if engineStatusTopic := os.Getenv("KAFKA_ENGINE_STATUS_TOPIC"); len(engineStatusTopic) > 0 {
		config.Messaging.MessageTopics.EngineStatus = engineStatusTopic
	}
	config.VeritoneAPI.BaseURL = payload.VeritoneApiBaseUrl

	if streamOutputTopic := os.Getenv("STREAM_OUTPUT_TOPIC"); len(streamOutputTopic) > 0 {
		config.OutputTopicName, config.OutputTopicPartition, config.OutputTopicKeyPrefix = messaging.ParseStreamTopic(streamOutputTopic)
	}
	if outputBucketName := os.Getenv("CHUNK_CACHE_BUCKET"); len(outputBucketName) > 0 {
		config.OutputBucketName = outputBucketName
	}
	if minioServer := os.Getenv("MINIO_SERVER"); len(minioServer) > 0 {
		config.MinioServer = minioServer
	}
	if region := os.Getenv("CHUNK_CACHE_AWS_REGION"); len(region) > 0 {
		config.OutputBucketRegion = region
	}

	config.VeritoneAPI.CorrelationID = engineID + ":" + config.EngineInstanceID
	config.VeritoneAPI.Timeout = graphQlTimeoutDuration

	return config, payload, config.validate()
}
