package controller

import (
	"context"
	"encoding/json"
	"github.com/veritone/engine-toolkit/engine/processing"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	owModels "github.com/veritone/realtime/modules/engines/outputWriter/models"
	"github.com/veritone/realtime/modules/engines/scfsio"
	"github.com/veritone/realtime/modules/engines/worker"
	"log"

	"github.com/Shopify/sarama"
	"time"
	/*
		"github.com/pkg/errors"
		"net/http"
		"bytes"
		"io"
		"strings"
		"mime"
		"mime/multipart"
		"github.com/veritone/engine-toolkit/engine/internal/vericlient"
		"io/ioutil"
	*/
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/veritone/engine-toolkit/engine/internal/vericlient"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
)

//here's what going to happen
// this will take in the same payload as the other base engines
// it will attempt to acquireChunk -- 1 at a time for N units to process
//    it will pull down the chunk's "media_x"  then create a method to process a `media-chunk message`
// based on engine.processMessageMediaChunk
//    The engine output which should be vtn-standard  --> go to chunk as raw file, make sure to have SourceEngineId and SourceTaskId associated with it
//     with the userMetadata to store the `engine-output` but without the content (vtn-standard output)
//
// we don't need to drop to chunk_all since the terminal would be output writer which should be started here as well, and it will look for chunks from FS
// What if we have engines producing non-vtn-standard ?? --> chunk
//

type ExternalEngineHandler struct {
	engineId              string
	engineInstanceId      string
	workItem              *controllerClient.EngineInstanceWorkItem
	workItemStatusManager scfsio.WorkItemStatusManager

	payloadJSON string

	inputIO  []scfsio.LocalSCFSIOInput
	outputIO []scfsio.LocalSCFSIOOutput

	totalRead     int64
	totalWritten  int64
	outputSummary map[string]interface{}

	// kafka stuff
	producer        processing.Producer
	kafkaChunkTopic string
	webhookConfig   processing.Webhooks

	// client is the client to use to make webhook requests.
	webhookClient *http.Client
	// graphQLHTTPClient is the client used to access GraphQL.
	graphQLHTTPClient *http.Client

	processingConfig processing.Processing
}

func NewExternalEngineHandler(payloadJSON string,
	engineInstanceId string,
	workItem *controllerClient.EngineInstanceWorkItem,
	workItemStatusManager scfsio.WorkItemStatusManager,
	inputIO []scfsio.LocalSCFSIOInput,
	outputIO []scfsio.LocalSCFSIOOutput,
	producer processing.Producer,
	kafkaChunkTopic string,
	webhookConfig processing.Webhooks,
	processingConfig processing.Processing,
	webhookClient *http.Client,
	graphQLHTTPClient *http.Client) (res worker.Worker, err error) {
	return &ExternalEngineHandler{
		engineInstanceId:      engineInstanceId,
		engineId:              workItem.EngineId,
		workItem:              workItem,
		workItemStatusManager: workItemStatusManager,
		payloadJSON:           payloadJSON,
		inputIO:               inputIO,
		outputIO:              outputIO,

		producer:         producer,
		kafkaChunkTopic:  kafkaChunkTopic,
		webhookConfig:    webhookConfig,
		processingConfig: processingConfig,

		webhookClient:     webhookClient,
		graphQLHTTPClient: graphQLHTTPClient,
	}, nil
}

// TODO start the heart beat for the task
// Heartbeat -- could have the info = the engine instance status update for the task
func (e *ExternalEngineHandler) startHeartbeat(ctx context.Context) {
	// placeholder
	log.Println("TODO TODO TODO HEARTBEAT FOR NON-CHUNK ENGINE")
}

func (e *ExternalEngineHandler) Stats() (totalRead int64, totalWritten int64, outputSummary map[string]interface{}) {
	return e.totalRead, e.totalWritten, e.outputSummary
}

func (e *ExternalEngineHandler) Run(ctx context.Context) (errReason worker.ErrorReason) {
	method := fmt.Sprintf("[externalEngineHandler.Run taskId=%s]", e.workItem.TaskId)
	if e.workItem.EngineType != "chunk" {
		// got to do heartbeat here baby
		// also todo stream engine!!!
		go e.startHeartbeat(ctx)
	}
	// just assume the first input to go to the first output for now
	var designatedInputIO *scfsio.LocalSCFSIOInput
	var designatedOutputIO *scfsio.LocalSCFSIOOutput
	if e.inputIO != nil {
		designatedInputIO = &e.inputIO[0]
	}
	if e.outputIO != nil {
		designatedOutputIO = &e.outputIO[0]
	}

	var i int32
	for i = 0; i < e.workItem.UnitCountToProcess; i++ {
		var inputChunk *scfsio.LocalSCFSChunkInput
		var err error
		var outputChunk *scfsio.LocalSCFSChunkOutput
		inputChunk, err = designatedInputIO.GetReader(ctx, "") // plain acquire chunks
		if err != nil {
			e.workItemStatusManager.ReportError(false, err, designatedInputIO.String(), "internal_error",
				fmt.Sprintf("%s Failed to get reader for input chunk", method))
			continue
		}

		inputChunkIndex := inputChunk.GetCurChunkContext()

		if designatedOutputIO != nil {
			outputChunk = designatedOutputIO.GetWriter(inputChunkIndex)
		}

		mediaChunk, err := e.getMediaChunk(inputChunk)
		if err != nil {
			e.workItemStatusManager.ReportError(false, err, designatedInputIO.String(), "internal_error",
				fmt.Sprintf("%s Failed to get reader for input chunk", method))
			continue
		}
		err = e.processChunk(ctx, mediaChunk, inputChunk, outputChunk)
	}
	/**
	we have work item.
	we have IO for input/output
	we have directive to process N chunks
	// do we want to handle stream engine here? not so since it will involev
	*/
	if e.outputSummary == nil {
		e.outputSummary = make(map[string]interface{})
	}
	if designatedInputIO != nil {
		e.totalRead, e.outputSummary["inputChunks"] = designatedInputIO.GetStats()
	}
	if designatedOutputIO != nil {
		e.totalWritten, e.outputSummary["outputChunks"] = designatedOutputIO.GetStats()
	}
	return errReason
}

func (e *ExternalEngineHandler) getMediaChunk(inputChunk *scfsio.LocalSCFSChunkInput) (mediaChunk *processing.MediaChunkMessage, err error) {
	// check for chunk MAIN_MESSAGE
	userMetadata := inputChunk.GetCurChunkInfo().GetUserMetadata()
	var chunkMainMessage interface{}
	if userMetadata != nil {
		chunkMainMessage = userMetadata[scfsio.CHUNK_MAIN_MESSAGE]
	}
	if chunkMainMessage == nil {
		return nil, fmt.Errorf("Unable to find %s in chunk %s usermetadata", scfsio.CHUNK_MAIN_MESSAGE, inputChunk.String())
	}
	bArr, err := json.Marshal(chunkMainMessage) // marshal this to byte array so we can unmarshal it to another type
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to read %s in chunk %s usermetadata", scfsio.CHUNK_MAIN_MESSAGE, inputChunk.String())
	}
	// theoretically engine output and media chunk share the same structure,
	// so here, we have the MediaChunkMessage is a union of both
	mediaChunk = new(processing.MediaChunkMessage)
	if err := json.Unmarshal(bArr, mediaChunk); err != nil {
		return nil, errors.Wrapf(err, "Failed to convert to media chunk for chunk %s", inputChunk.String())
	}
	// correct the TaskId since it's no lone need to map this media chunk that was from the `parent` to this task
	mediaChunk.TaskID = e.workItem.TaskId
	mediaChunk.TaskPayload = json.RawMessage(e.payloadJSON)
	return mediaChunk, nil
}

/**
get a chunk
call engine

publish to kafka as `ChunkResult`
write a chunk + userMetadata =
        main-message = engineOutput?

*/
func (e *ExternalEngineHandler) processChunk(ctx context.Context, mediaChunk *processing.MediaChunkMessage,
	inputChunk *scfsio.LocalSCFSChunkInput, outputChunk *scfsio.LocalSCFSChunkOutput) (err error) {

	finalUpdateMessage := processing.ChunkResult{
		Type:      processing.MessageTypeChunkResult,
		TaskID:    mediaChunk.TaskID,
		ChunkUUID: mediaChunk.ChunkUUID,
		Status:    processing.ChunkStatusSuccess, // optimistic
	}
	defer func() {
		if err != nil {
			// mark the chunk input as having error
			inputChunk.Close(false, errors.Wrapf(err, "Failed to process Chunk=%s", inputChunk.String()))
			return
		}
		// if need to send to Kafka then the producer would be non-nil
		if e.producer != nil {
			finalUpdateMessage.TimestampUTC = time.Now().Unix()
			_, _, kafkaErr := e.producer.SendMessage(&sarama.ProducerMessage{
				Topic: e.kafkaChunkTopic,
				Key:   sarama.ByteEncoder(mediaChunk.TaskID),
				Value: processing.NewJSONEncoder(finalUpdateMessage),
			})
			if kafkaErr != nil {
				log.Printf("IGNORE .. failed to send final chunk update to Kafka:", kafkaErr)
			}
		}
		// For now only write out engineOutput..
		if finalUpdateMessage.EngineOutput != nil && outputChunk != nil {
			engineOutput := finalUpdateMessage.EngineOutput

			if engineOutput.Content != "" {
				//let's write to the chunk
				outputChunk.Write([]byte(engineOutput.Content))
			}
			engineOutput.Content = "" // don't save this..
			outputChunk.AddUserMetadata(scfsio.CHUNK_MAIN_MESSAGE, engineOutput)
			outputChunk.Close()
		}
	}()

	ignoreChunk := false

	retry := processing.NewDoubleTimeBackoff(
		e.webhookConfig.Backoff.InitialBackoffDuration,
		e.webhookConfig.Backoff.MaxBackoffDuration,
		e.webhookConfig.Backoff.MaxRetries,
	)
	// todo: see if we can host the input data within the engine toolkit
	// in an HTTP endpoint --
	// currently the cacheUri in the media message is the controller chunk cache uri
	// this is only temporary to accommodate v2f engines.
	// for v3 -- we should have it directly retrieved here.

	var content string
	err = retry.Do(func() error {
		req, err := processing.NewRequestFromMediaChunk(e.webhookClient, e.webhookConfig.Process.URL,
			*mediaChunk, e.processingConfig.DisableChunkDownload)
		if err != nil {
			return errors.Wrap(err, "new request")
		}
		req = req.WithContext(ctx)
		resp, err := e.webhookClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			ignoreChunk = true
			return nil
		}
		if resp.StatusCode != http.StatusOK {
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, resp.Body); err != nil {
				return errors.Wrap(err, "read body")
			}
			return errors.Errorf("%d: %s", resp.StatusCode, strings.TrimSpace(buf.String()))
		}
		if resp.ContentLength == 0 {
			ignoreChunk = true
			return nil
		}
		mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil {
			log.Println("content type parsing failed, assuming json:", err)
		}
		if strings.HasPrefix(mediaType, "multipart/") {
			// files output
			payload, err := mediaChunk.UnmarshalPayload()
			if err != nil {
				return errors.Wrap(err, "unmarshal payload")
			}
			type mediaItem struct {
				AssetID     string `json:"assetId"`
				ContentType string `json:"contentType"`
			}
			var outputJSON struct {
				Media []mediaItem `json:"media"`
			}
			mr := multipart.NewReader(resp.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrap(err, "reading multipart response")
				}

				// missing assetType ...
				assetCreate := processing.AssetCreate{
					ContainerTDOID: mediaChunk.TDOID,
					ContentType:    p.Header.Get("Content-Type"),
					Name:           p.FileName(),
					Body:           p,
				}
				client := vericlient.NewClient(e.graphQLHTTPClient, payload.Token, payload.VeritoneAPIBaseURL+"/v3/graphql")
				createdAsset, err := assetCreate.Do(ctx, client)
				if err != nil {
					return errors.Wrapf(err, "create asset for %s", p.FileName())
				}
				outputJSON.Media = append(outputJSON.Media, mediaItem{
					AssetID:     createdAsset.ID,
					ContentType: createdAsset.ContentType,
				})
				return nil
			}
			jsonBytes, err := json.Marshal(outputJSON)
			if err != nil {
				return errors.Wrap(err, "encode output JSON")
			}
			content = string(jsonBytes)
		} else {
			// JSON output
			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return errors.Wrap(err, "read response body")
			}
			var vtnStandardEngineOutput owModels.VtnStandardOutput
			err = json.Unmarshal(bodyBytes, &vtnStandardEngineOutput)
			if err == nil {
				// assign SourceEngineId and TaskId
				vtnStandardEngineOutput.TaskId = e.workItem.TaskId
				vtnStandardEngineOutput.SourceEngineId = e.workItem.EngineId
				// ok deseralize back
				jsonBytes, err := json.Marshal(vtnStandardEngineOutput)
				if err == nil {
					bodyBytes = jsonBytes
				}
			}
			content = string(bodyBytes)
		}
		return nil
	})
	if err != nil {
		// send error message
		finalUpdateMessage.Status = processing.ChunkStatusError
		finalUpdateMessage.ErrorMsg = err.Error()
		finalUpdateMessage.FailureReason = "internal_error"
		finalUpdateMessage.FailureMsg = finalUpdateMessage.ErrorMsg
		return err
	}
	if ignoreChunk {
		finalUpdateMessage.Status = processing.ChunkStatusIgnored
		err = nil
		return err
	}
	// send output message
	outputMessage := processing.MediaChunkMessage{
		Type:          processing.MessageTypeEngineOutput,
		TaskID:        mediaChunk.TaskID,
		JobID:         mediaChunk.JobID,
		ChunkUUID:     mediaChunk.ChunkUUID,
		StartOffsetMS: mediaChunk.StartOffsetMS,
		EndOffsetMS:   mediaChunk.EndOffsetMS,
		TimestampUTC:  time.Now().Unix(),
		Content:       content,
	}
	tmp, _ := json.Marshal(outputMessage)
	log.Println("outputMessage will be sent to kafka: ", string(tmp))
	finalUpdateMessage.TimestampUTC = time.Now().Unix()
	finalUpdateMessage.EngineOutput = &outputMessage

	err = nil // set up so defer can check on it
	return err
}
