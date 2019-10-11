package SIv2

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/veritone/edge-stream-ingestor/api"
	"github.com/veritone/edge-stream-ingestor/ingest"
	"github.com/veritone/edge-stream-ingestor/messaging"
)

const (
	defaultHeartbeatInterval      = "5s"
	defaultConfigFilePath         = "./config.json"
	envVarMinioServer             = "MINIO_SERVER"
	envVarMinioServerURLIPBased   = "MINIO_SERVER_URL_IP_BASED"
	envVarMinioMediaSegmentBucket = "MINIO_MEDIA_SEGMENT_BUCKET"
	envVarAwsKeyID                = "AWS_ACCESS_KEY_ID"
	defaultGlobalTimeout          = "30m"
)

var (
	payloadFlag       = flag.String("payload", "", "payload file")
	configFlag        = flag.String("config", "", "config file")
	tdoIDFlag         = flag.String("tdo", "", "Temporal data object ID (override payload)")
	createTDOFlag     = flag.Bool("create-tdo", false, "Create a new temporal data object")
	urlFlag           = flag.String("url", "", "Override URL to ingest")
	stdinFlag         = flag.Bool("stdin", false, "Read stream from stdin pipe")
	stdinMimeTypeFlag = flag.String("input-mimetype", "", "MIME-Type of stream on stdin")
	stdinFormatFlag   = flag.String("input-format", "", "FFMPEG format of stream on stdin")
	stdoutFlag        = flag.Bool("stdout", false, "Write stream to stdout")
	outputFlag        = flag.String("out", "", "A file path to write the output stream to")
	transcodeFlag     = flag.String("transcode", "", "FFMPEG format to transcode output stream to")
	rawAssetFlag      = flag.Bool("save-raw", false, "Save raw stream as primary asset")
)

func init() {
	flag.Parse()
}

type engineConfig struct {
	ingest.IngestorConfig
	EngineID               string                    `json:"engineId,omitempty"`
	EngineInstanceID       string                    `json:"engineInstanceId,omitempty"`
	VeritoneAPI            api.Config                `json:"veritoneAPI,omitempty"`
	Messaging              messaging.ClientConfig    `json:"messaging,omitempty"`
	AssetStorage           ingest.AssetStorageConfig `json:"assetStorage,omitempty"`
	HeartbeatInterval      string                    `json:"heartbeatInterval,omitempty"`
	OutputTopicName        string                    `json:"outputTopicName,omitempty"`
	OutputTopicPartition   int32                     `json:"outputTopicPartition"`
	OutputTopicKeyPrefix   string                    `json:"outputTopicKeyPrefix"`
	InputTopicName         string                    `json:"inputTopicName,omitempty"`
	InputTopicPartition    int32                     `json:"inputTopicPartition"`
	InputTopicKeyPrefix    string                    `json:"inputTopicKeyPrefix"`
	SupportedTextMimeTypes []string                  `json:"supportedTextMimeTypes,omitempty"`
	GlobalTimeout          string                    `json:"globalTimeout,omitempty"`
}

func (c *engineConfig) String() string {
	j, _ := json.Marshal(c)
	return string(j)
}

func (c *engineConfig) validate() error {
	if _, err := time.ParseDuration(c.HeartbeatInterval); err != nil {
		return fmt.Errorf(`invalid value for "heartbeatInterval": "%s" - %s`, c.HeartbeatInterval, err)
	}

	if _, err := time.ParseDuration(c.SegmentDuration); err != nil {
		return fmt.Errorf(`invalid value for "segmentDuration": "%s" - %s`, c.SegmentDuration, err)
	}

	return nil
}

func (c *engineConfig) defaults() {
	c.IngestorConfig.Defaults()
	if c.HeartbeatInterval == "" {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.GlobalTimeout == "" {
		c.GlobalTimeout = defaultGlobalTimeout
	}
}

func (c *engineConfig) applyOverrides(payload enginePayload) {
	c.GenerateMediaAssets = payload.GenerateMediaAssets
	c.OutputRawAsset = payload.OutputRawAsset
	c.FrameExtraction.ExtractFramesPerSec = payload.ExtractFramesPerSec

	if payload.SaveRawAsset != nil {
		c.SaveRawAsset = *payload.SaveRawAsset
	}
	if payload.OutputChunkDuration != "" {
		c.Chunking.ChunkDuration = payload.OutputChunkDuration
	}
	if payload.ChunkOverlapDuration != "" {
		c.Chunking.ChunkOverlapDuration = payload.ChunkOverlapDuration
	}
	if payload.SegmentDuration != "" {
		c.SegmentDuration = payload.SegmentDuration
	}
	if payload.OutputAudioChunks {
		c.Chunking.ProduceAudioChunks = true
	}
	if payload.OutputVideoChunks {
		c.Chunking.ProduceVideoChunks = true
	}
	if payload.OutputImageChunks {
		c.Chunking.ProduceImageChunks = true
	}
}

type inputPayload struct {
	OrgID  string `json:"orgId,omitempty"`
	JobID  string `json:"jobId,omitempty"`
	TaskID string `json:"taskId,omitempty"`
}

type taskPayload struct {
	URL                  string    `json:"url,omitempty"`
	StreamStartTime      time.Time `json:"streamStartTime,omitempty"`
	GenerateMediaAssets  bool      `json:"generateMediaAssets"`
	SaveRawAsset         *bool     `json:"saveRawAsset"`
	OutputRawAsset       bool      `json:"outputRawAsset"`
	OutputAudioChunks    bool      `json:"outputAudioChunks"`
	OutputVideoChunks    bool      `json:"outputVideoChunks"`
	OutputImageChunks    bool      `json:"outputImageChunks"`
	SegmentDuration      string    `json:"segmentDuration,omitempty"`
	OutputChunkDuration  string    `json:"outputChunkDuration,omitempty"`
	ChunkOverlapDuration string    `json:"chunkOverlapDuration,omitempty"`
	ExtractFramesPerSec  float64   `json:"extractFramesPerSec,omitempty"`
	TranscodeFormat      string    `json:"transcodeFormat,omitempty"`
	OrganizationID       int64     `json:"organizationId,omitempty"`
	inputPayload         `json:"input,omitempty"`
}

type enginePayload struct {
	JobID       string `json:"jobId,omitempty"`
	TaskID      string `json:"taskId,omitempty"`
	TDOID       string `json:"recordingId,omitempty"`
	Token       string `json:"token,omitempty"`
	taskPayload `json:"taskPayload,omitempty"`
}

func (p enginePayload) String() string {
	// redact Token field so it doesn't leak into log
	p.Token = ""
	j, _ := json.Marshal(p)
	return string(j)
}

func (p *enginePayload) defaults() {
	if tdoIDFlag != nil && *tdoIDFlag != "" {
		p.TDOID = *tdoIDFlag
	}
	if urlFlag != nil && *urlFlag != "" {
		p.URL = *urlFlag
	}
	if transcodeFlag != nil && *transcodeFlag != "" {
		p.TranscodeFormat = *transcodeFlag
	}
	if rawAssetFlag != nil && *rawAssetFlag {
		*p.SaveRawAsset = true
	}
}

func (p *enginePayload) validate() error {
	if !*createTDOFlag && p.TDOID == "" {
		return errors.New("Missing tdoId from payload")
	}
	if p.JobID == "" {
		return errors.New("Missing jobId from payload")
	}
	if p.TaskID == "" {
		return errors.New("Missing taskId from payload")
	}
	if !*stdinFlag && p.URL == "" && os.Getenv("STREAM_INPUT_TOPIC") == "" {
		return errors.New("either a stream input topic or URL is required")
	}

	if p.OutputChunkDuration != "" {
		if _, err := time.ParseDuration(p.OutputChunkDuration); err != nil {
			return fmt.Errorf(`invalid value given for "outputChunkDuration": %q - %s`, p.OutputChunkDuration, err)
		}
	}
	if p.ChunkOverlapDuration != "" {
		if _, err := time.ParseDuration(p.ChunkOverlapDuration); err != nil {
			return fmt.Errorf(`invalid value given for "chunkOverlapDuration": %q - %s`, p.ChunkOverlapDuration, err)
		}
	}

	return nil
}

func loadPayloadFromFile(p interface{}, payloadFilePath string) error {
	if payloadFilePath == "" {
		return errors.New("no payload file specified")
	}

	r, err := os.Open(payloadFilePath)
	if err != nil {
		return err
	}
	defer r.Close()
	return json.NewDecoder(r).Decode(p)
}

func loadConfigFromFile(c interface{}, configFilePath string) error {
	if configFilePath == "" {
		configFilePath = defaultConfigFilePath
	}

	r, err := os.Open(configFilePath)
	if err != nil {
		return err
	}
	defer r.Close()
	return json.NewDecoder(r).Decode(c)
}

func loadConfigAndPayload() (*engineConfig, *enginePayload, error) {
	payload, config := new(enginePayload), new(engineConfig)

	// parse payload from environment variable, if set, else load from file
	if payloadJSON := os.Getenv("PAYLOAD_JSON"); len(payloadJSON) > 0 {
		fmt.Println("JSON Payload: " + payloadJSON)
		if err := json.Unmarshal([]byte(payloadJSON), payload); err != nil {
			return config, payload, fmt.Errorf("failed to unmarshal payload JSON: %s", err)
		}
	} else {
		err := loadPayloadFromFile(payload, *payloadFlag)
		if err != nil {
			return config, payload, fmt.Errorf("failed to load payload file: %s", err)
		}
	}

	payload.defaults()
	if err := payload.validate(); err != nil {
		return config, payload, fmt.Errorf("invalid payload: %s", err)
	}

	// load config from file
	if err := loadConfigFromFile(config, *configFlag); err != nil {
		return config, payload, err
	}
	config.defaults()

	if engineID := os.Getenv("ENGINE_ID"); len(engineID) > 0 {
		config.EngineID = engineID
	}
	if engineInstanceID := os.Getenv("ENGINE_INSTANCE_ID"); len(engineInstanceID) > 0 {
		config.EngineInstanceID = engineInstanceID
	}
	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); len(kafkaBrokers) > 0 {
		config.Messaging.Kafka.Brokers = strings.Split(kafkaBrokers, ",")
	}
	if chunkQueueTopic := os.Getenv("KAFKA_CHUNK_TOPIC"); len(chunkQueueTopic) > 0 {
		config.Messaging.MessageTopics.ChunkQueue = chunkQueueTopic
	}
	if heartbeatTopic := os.Getenv("KAFKA_HEARTBEAT_TOPIC"); len(heartbeatTopic) > 0 {
		config.Messaging.MessageTopics.EngineStatus = heartbeatTopic
	}
	if apiBaseURL := os.Getenv("VERITONE_API_BASE_URL"); len(apiBaseURL) > 0 {
		config.VeritoneAPI.BaseURL = apiBaseURL
	}
	if chunkCacheBucket := os.Getenv("CHUNK_CACHE_BUCKET"); len(chunkCacheBucket) > 0 {
		config.Chunking.Cache.Bucket = chunkCacheBucket
	}
	if chunkCacheRegion := os.Getenv("CHUNK_CACHE_AWS_REGION"); len(chunkCacheRegion) > 0 {
		config.Chunking.Cache.Region = chunkCacheRegion
	}
	if config.Messaging.Kafka.ClientIDPrefix == "" {
		config.Messaging.Kafka.ClientIDPrefix = appName + "_"
	}
	if streamOutputTopic := os.Getenv("STREAM_OUTPUT_TOPIC"); len(streamOutputTopic) > 0 {
		config.OutputTopicName, config.OutputTopicPartition, config.OutputTopicKeyPrefix = messaging.ParseStreamTopic(streamOutputTopic)
	}
	if streamInputTopic := os.Getenv("STREAM_INPUT_TOPIC"); len(streamInputTopic) > 0 {
		config.InputTopicName, config.InputTopicPartition, config.InputTopicKeyPrefix = messaging.ParseStreamTopic(streamInputTopic)
	}
	if minioServer := os.Getenv(envVarMinioServer); len(minioServer) > 0 {
		config.Chunking.Cache.MinioServer = minioServer
		config.AssetStorage.MinioStorage.MinioServer = minioServer
		logger.Printf("minio server: %s", config.Chunking.Cache.MinioServer)
		logger.Printf("minio server set into Media Segment Config: %s", config.AssetStorage.MinioStorage.MinioServer)
		logger.Printf("access key: %s", os.Getenv(envVarAwsKeyID))
	}
	//This env. variable would be coming from SEM for Portable Edge Flow. Please check the docker-compose.yml file's SEM service section
	//This IP based URL is used for asset upload into Minio
	if minioServerURLIPBased := os.Getenv(envVarMinioServerURLIPBased); len(minioServerURLIPBased) > 0 {
		config.AssetStorage.MinioStorage.MinioServerURLIPBased = minioServerURLIPBased
		logger.Printf("minio server ip based: %s", config.AssetStorage.MinioStorage.MinioServerURLIPBased)
	}
	//This env. variable would be coming from SEM for Portable Edge Flow. Please check the docker-compose.yml file's SEM service section
	//This is the minio bucket into which the Asset would beuploaded
	if minioMediaSegmentBucket := os.Getenv(envVarMinioMediaSegmentBucket); len(minioMediaSegmentBucket) > 0 {
		config.AssetStorage.MinioStorage.MediaSegmentBucket = minioMediaSegmentBucket
		logger.Printf("minio server media segment bucket: %s", config.AssetStorage.MinioStorage.MediaSegmentBucket)
	}

	config.VeritoneAPI.CorrelationID = config.EngineInstanceID

	return config, payload, config.validate()
}
