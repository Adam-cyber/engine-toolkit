package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/veritone/engine-toolkit/engine/processing"
)

const (
	eventType = "edge_event_data"

	// eventConsumed event when consumed a Kafka message from its engine topic
	eventConsumed = "media_chunk_consumed"
	// eventProduced (or ChunkProcessedStatusProduced or EngineOutputProduced) event when producing a Kafka message to chunk_all topic
	eventProduced = "chunk_result_produced"
	// eventStart when engine instance is ready to process work
	eventStart = "engine_instance_ready"
	// eventStop when engine instance gracefully shutdown
	eventStop = "engine_instance_quit"
	// eventPeriodic event reporting total time (rounded to nearest second) engine instance has been and total time processing
	eventPeriodic = "engine_instance_up_periodic"
)

// event is an event that is sent to the platform.
type event struct {
	// Key for the topic
	Key string
	// Type of the event should be one of the constants
	Type string

	// for produce/consume events
	JobID   string
	TaskID  string
	ChunkID string

	// for periodic events
	ProcessingDurationSecs int64
	UpDurationSecs         int64
}

// sendEvent produces an event with fire and forget policy
func (e *Engine) sendEvent(evt event) {
	if e.eventProducer == nil {
		e.logDebug(fmt.Sprintf("(skipping) sendEvent: %+v", evt))
		return
	}
	e.logDebug(fmt.Sprintf("sendEvent: %+v", evt))
	buildID := strings.Replace(e.Config.Kafka.ChunkTopic, "chunk_in_", "", -1)
	now := int64(time.Now().UTC().UnixNano() / 1e6)
	edgeEvt := edgeEvent{
		Type:         eventType,
		TimestampUTC: now,
		EventTimeUTC: now,
		Component:    e.Config.Engine.ID,
		EngineInfo: &EngineInfo{
			EngineID:               e.Config.Engine.ID,
			InstanceID:             e.Config.Engine.InstanceID,
			BuildID:                buildID,
			ProcessingDurationSecs: evt.ProcessingDurationSecs,
			UpDurationSecs:         evt.UpDurationSecs,
		},
		Event:   evt.Type,
		JobID:   evt.JobID,
		TaskID:  evt.TaskID,
		ChunkID: evt.ChunkID,
	}
	_, _, err := e.eventProducer.SendMessage(&sarama.ProducerMessage{
		Topic: e.Config.Kafka.EventTopic,
		Key:   sarama.ByteEncoder(evt.Key),
		Value: processing.NewJSONEncoder(edgeEvt),
	})
	if err != nil {
		e.logDebug("WARN", "failed to produce engine event:", err, evt)
	}
}

// sendPeriodicEvents sends the periodic events.
// Cancellable via the context.
func (e *Engine) sendPeriodicEvents(ctx context.Context) {
	if e.Config.Events.PeriodicUpdateDuration == 0 {
		e.logDebug("(skipping) sendPeriodicEvents because Config.Events.PeriodicUpdateDuration == 0")
		return
	}
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.Config.Events.PeriodicUpdateDuration):
			now := time.Now()
			e.sendEvent(event{
				Key:                    e.Config.Engine.ID,
				Type:                   eventPeriodic,
				UpDurationSecs:         int64(now.Sub(start).Seconds()),
				ProcessingDurationSecs: int64(e.ProcessingDuration().Seconds()),
			})
		}
	}
}

// edgeEvent is the contract to send the data to kafka
type edgeEvent struct {
	Type         string      `json:"type,omitempty"`         // EdgeEventDataType (required)
	TimestampUTC int64       `json:"timestampUTC,omitempty"` // TimestampUTC is time this message created in epoch millisecs (optional)
	Event        string      `json:"event,omitempty"`        // Event is one of Edge events listed above (required)
	EventTimeUTC int64       `json:"eventTimeUTC,omitempty"` // EventTimeUTC is time that event occurred in epoch millisecs (required)
	Component    string      `json:"component,omitempty"`    // Component is service name or EngineID that generated the event (required)
	JobID        string      `json:"jobId,omitempty"`        // JobID this event is associated with (required, except for EngineInstance* events)
	TaskID       string      `json:"taskId,omitempty"`       // TaskID this event is associated with (required, except for EngineInstance* events)
	ChunkID      string      `json:"chunkId,omitempty"`      // ChunkID this event is associated with (required, except for GQLCall, ChunkEOF, GenericEvent, RunTask, and EngineInstance events)
	EngineInfo   *EngineInfo `json:"engineInfo,omitempty"`   // EngineInfo required only for EngineInstance events
}

// EngineInfo contains contextual data for EngineInstance* events
type EngineInfo struct {
	EngineID               string `json:"engineId,omitempty"`               // EngineID of instance (required)
	BuildID                string `json:"buildId,omitempty"`                // BuildID of instance (required)
	InstanceID             string `json:"instanceId,omitempty"`             // InstanceID is unique ID of instance (either AWS Task ID or random UUID) (required)
	UpDurationSecs         int64  `json:"upDurationSecs,omitempty"`         // UpDurationSecs Required only for EngineInstancePeriodic event, contains engine up time in seconds
	ProcessingDurationSecs int64  `json:"processingDurationSecs,omitempty"` // ProcessingDurationSecs required only for EngineInstancePeriodic event, contains engine processing time in seconds
}
