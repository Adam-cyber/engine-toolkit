package controller

import (
	"github.com/veritone/realtime/modules/engines/scfsio"
	"github.com/veritone/realtime/modules/engines/worker"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"log"
	"context"
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
	engineId         string
	engineInstanceId string
	workItem         *controllerClient.EngineInstanceWorkItem
	workItemStatusManager scfsio.WorkItemStatusManager

	payloadJSON  string

	inputIO       []scfsio.LocalSCFSIOInput
	outputIO      []scfsio.LocalSCFSIOOutput

	totalRead     int64
	totalWritten  int64
	outputSummary map[string]interface{}
}

func NewExternalEngineHandler (payloadJSON string,
	engineInstanceId string,
	workItem *controllerClient.EngineInstanceWorkItem,
	workItemStatusManager scfsio.WorkItemStatusManager,
	inputIO []scfsio.LocalSCFSIOInput,
	outputIO []scfsio.LocalSCFSIOOutput) (res worker.Worker, err error){
	return &ExternalEngineHandler{
		engineInstanceId:engineInstanceId,
		engineId: workItem.EngineId,
		workItem : workItem,
		workItemStatusManager: workItemStatusManager,
		payloadJSON: payloadJSON,
		inputIO: inputIO,
		outputIO: outputIO,
	}, nil
}


// TODO start the heart beat for the task
// Heartbeat -- could have the info = the engine instance status update for the task
func (e *ExternalEngineHandler) startHeartbeat(ctx context.Context) {
	// placeholder
	log.Println("TODO TODO TODO HEARTBEAT FOR NON-CHUNK ENGINE")
}

func (e *ExternalEngineHandler) Stats () (totalRead int64, totalWritten int64, outputSummary map[string]interface{}) {
	return
}
func (e *ExternalEngineHandler) Run (ctx context.Context) (errReason worker.ErrorReason) {
	if e.workItem.EngineType != "chunk" {
		// got to do heartbeat here baby
		go e.startHeartbeat(ctx)
	}

	/**
	we have work item.
	we have IO for input/output
	we have directive to process N chunks
	// do we want to handle stream engine here? not so since it will involev
	 */
	return errReason
}

/*
// processMessageMediaChunk processes a single media chunk as described by the sarama.ConsumerMessage.
func (e *Engine) processMessageMediaChunk(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var mediaChunk processing.MediaChunkMessage
	if err := json.Unmarshal(msg.Value, &mediaChunk); err != nil {
		return errors.Wrap(err, "unmarshal message value JSON")
	}
	e.sendEvent(event{
		Key:     mediaChunk.ChunkUUID,
		Type:    eventConsumed,
		JobID:   mediaChunk.JobID,
		TaskID:  mediaChunk.TaskID,
		ChunkID: mediaChunk.ChunkUUID,
	})
	finalUpdateMessage := processing.ChunkResult{
		Type:      processing.MessageTypeChunkResult,
		TaskID:    mediaChunk.TaskID,
		ChunkUUID: mediaChunk.ChunkUUID,
		Status:    processing.ChunkStatusSuccess, // optimistic
	}
	defer func() {
		// send the final (ChunkResult) message
		finalUpdateMessage.TimestampUTC = time.Now().Unix()
		_, _, err := e.producer.SendMessage(&sarama.ProducerMessage{
			Topic: e.Config.Kafka.ChunkTopic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: newJSONEncoder(finalUpdateMessage),
		})
		if err != nil {
			e.logDebug("WARN", "failed to send final chunk update:", err)
		}
		e.sendEvent(event{
			Key:     mediaChunk.ChunkUUID,
			Type:    eventProduced,
			JobID:   mediaChunk.JobID,
			TaskID:  mediaChunk.TaskID,
			ChunkID: mediaChunk.ChunkUUID,
		})
	}()
	ignoreChunk := false
	retry := newDoubleTimeBackoff(
		e.Config.Webhooks.Backoff.InitialBackoffDuration,
		e.Config.Webhooks.Backoff.MaxBackoffDuration,
		e.Config.Webhooks.Backoff.MaxRetries,
	)
	var content string
	err := retry.Do(func() error {
		req, err := processing.NewRequestFromMediaChunk(e.webhookClient, e.Config.Webhooks.Process.URL,
			mediaChunk, e.Config.Processing.DisableChunkDownload)
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
			e.logDebug("content type parsing failed, assuming json:", err)
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
				// todo edge514- what's the user case here what's the assetType?
				assetCreate := AssetCreate{
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
		return nil
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
	e.logDebug("outputMessage will be sent to kafka: ", string(tmp))
	finalUpdateMessage.TimestampUTC = time.Now().Unix()
	finalUpdateMessage.EngineOutput = &outputMessage
	return nil
}

 */
