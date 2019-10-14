package siv2

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	messages "github.com/veritone/edge-messages"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2/messaging"
	"github.com/veritone/engine-toolkit/engine/internal/controller/scfsio/streamio"
	"github.com/veritone/engine-toolkit/engine/internal/controller/worker"
)

var lastStreamInfo map[string]interface{}

type bytesReadTracker interface {
	BytesRead() int64
}

type bytesWrittenTracker interface {
	BytesWritten() int64
}

type outputTracker interface {
	ObjectsSent() int
	OutputSummary() map[string]interface{}
}

type heartbeat struct {
	sync.Mutex
	bytesReadTracker
	bytesWrittenTracker
	outputTracker
	messageClient   *messaging.Helper
	index           int64
	engineStartTime time.Time
	ingestStartTime time.Time
	infoc           chan map[string]interface{}
}

func startHeartbeat(ctx context.Context, messageClient *messaging.Helper, heartbeatInterval time.Duration) *heartbeat {
	hb := &heartbeat{
		messageClient:   messageClient,
		engineStartTime: time.Now(),
		infoc:           make(chan map[string]interface{}, 1),
	}

	// send the first heartbeat
	err := hb.sendHeartbeat(ctx, messages.EngineStatusRunning, worker.ErrorReason{}, nil)
	if err != nil {
		logger.Println("WARNING: heartbeat message failed:", err)
	}

	go func() {
		var info map[string]interface{}

		for {
			select {
			case <-ctx.Done():
				return
			case info = <-hb.infoc: // info sent on this chan is sent with the next heartbeat
				lastStreamInfo = info
			case <-time.Tick(heartbeatInterval):
				err := hb.sendHeartbeat(ctx, messages.EngineStatusRunning, worker.ErrorReason{}, info)
				if err != nil {
					logger.Println("WARNING: heartbeat message failed:", err)
				}
				info = nil // reset so it doesn't get sent on the next heartbeat
			}
		}
	}()

	return hb
}

func (h *heartbeat) trackStream(stream *streamio.Stream) {
	h.Lock()
	defer h.Unlock()
	h.bytesReadTracker = stream
	h.ingestStartTime = time.Now()
	h.infoc <- stream.StreamProfile()
}

func (h *heartbeat) trackWrites(w bytesWrittenTracker) {
	h.Lock()
	defer h.Unlock()
	h.bytesWrittenTracker = w

	if ct, ok := w.(outputTracker); ok {
		h.outputTracker = ct
	}
}

func (h *heartbeat) sendHeartbeat(ctx context.Context, status messages.EngineStatus, err worker.ErrorReason, info map[string]interface{}) error {
	h.Lock()
	defer h.Unlock()

	h.index++
	msg := messages.EmptyEngineHeartbeat()
	msg.Count = h.index
	msg.Status = status
	msg.UpTime = int64(time.Now().Sub(h.engineStartTime) / time.Millisecond)

	if h.bytesReadTracker != nil {
		msg.BytesRead = h.BytesRead()
	}
	if h.bytesWrittenTracker != nil {
		msg.BytesWritten = h.BytesWritten()
	}

	// generate info message
	if info == nil {
		info = make(map[string]interface{})
	}
	if !h.ingestStartTime.IsZero() {
		dur := time.Now().Sub(h.ingestStartTime)
		info["ingestionTime"] = dur.String()
	}
	if h.outputTracker != nil {
		msg.MessagesWritten = int64(h.ObjectsSent())
		for key, val := range h.OutputSummary() {
			info[key] = val
		}
	}
	if len(info) > 0 {
		infoJSONBytes, _ := json.Marshal(info)
		msg.InfoMsg = string(infoJSONBytes)
	}

	if err.Err != nil {
		msg.ErrorMsg = err.Err.Error()
		msg.FailureMsg = err.Err.Error()
		msg.FailureReason = err.FailureReason
	}

	if msg.Status == messages.EngineStatusDone || msg.Status == messages.EngineStatusFailed {
		msgJSON, _ := json.Marshal(msg)
		logger.Printf("Sending final heartbeat: status: %s body: %s", string(status), string(msgJSON))
	}

	if h.bytesWrittenTracker != nil {
		bw := datasize.ByteSize(h.BytesWritten()) * datasize.B

		if h.outputTracker != nil {
			outputSummary := h.OutputSummary()
			chunks, _ := outputSummary["chunks"]
			assets, _ := outputSummary["assets"]
			durationIngested, ok := outputSummary["durationIngested"]
			if !ok {
				durationIngested = "0s"
			}

			logger.Printf("-- %s written | %s ingested | assets: %v | chunks: %v", bw.HR(), durationIngested, assets, chunks)
		} else {
			logger.Printf("-- %s written", bw.HR())
		}
	}

	return h.messageClient.ProduceHeartbeatMessage(ctx, &msg)
}
