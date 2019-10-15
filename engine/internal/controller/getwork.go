package controller

import (
	"context"
	"github.com/antihax/optional"
	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"log"
	"time"

	"fmt"
	"github.com/veritone/engine-toolkit/engine/internal/controller/stream-ingestor-v2"
	"github.com/veritone/engine-toolkit/engine/internal/controller/worker"
)

/**

Stream based --> referential based -> no concurrent
cannot stream parallel processing on the same route..


Cannot have parallel processing in a stream route


Multiple inputs --> correlation between 2 input folders to task

* should always have a control input source --> which can be  taken in any order, and when finished --> marked as done
* The others should have the option to be staying around, and up to engine to `purge` it



Chunks can be in random order or marked as time-based... so when the engines need to get the input

Need to be able to do multiple inputs with one primary input -->
and the engine itself could ask for input data from the other sources ..
somehow

*/

/**
ask Controller for more work.
Possible action is to `wait` or to `process` or error
for error and wait -- will sleep and retry until timeout, at that point may want to do the final update and bailed out?
*/
func (c *ControllerUniverse) AskForWork(ctx context.Context) (done bool, waitForMore bool, nItems int, err error) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()

	method := fmt.Sprintf("[ControllerUniverse.AskForWork:%s]", c.engineInstanceId)
	log.Println(method, "ENTER")
	defer log.Println(method, "EXIT")

	headerOpts := &controllerClient.GetEngineInstanceWorkOpts{
		XCorrelationId: optional.NewInterface(c.correlationId),
	}

	c.priorTimestamp = time.Now().Unix()
	curEngineWorkRequest := controllerClient.EngineInstanceWorkRequest{
		WorkRequestId:      c.curWorkRequestId,
		WorkRequestStatus:  c.curWorkRequestStatus,
		WorkRequestDetails: c.curWorkRequestDetails,
		HostId:             c.engineInstanceInfo.HostId,
		HostAction:         c.curHostAction,
		RequestWorkForEngineIds: c.requestWorkForEngineIds,
		TaskStatus:         c.curTaskStatusUpdatesForTheBatch,
		ContainerStatus:    c.curContainerStatus,
	}
	res, _, err := c.controllerAPIClient.EngineApi.GetEngineInstanceWork(
		context.WithValue(ctx, controllerClient.ContextAccessToken,
			c.engineInstanceRegistrationInfo.EngineInstanceToken),
		c.engineInstanceId,
		curEngineWorkRequest, headerOpts)
	if err != nil {
		// If there's an error -- probably need to retry in a few seconds
		// much like the `wait`
		log.Printf("%s returning err=%v", method, err)
		return false, false, 0, err
	}

	log.Printf("%s got action=%s", method, res.Action)
	switch res.Action {
	case workRequestActionTerminate:
		// bye bye
		return true, false, 0, nil // TODO also put some thing into some channel to say that we're done?
	case workRequestActionWait:
		return false, true, 0, nil
	}
	// now we have a new batch of work, reset the taskStatus

	c.curWorkRequestId = res.WorkRequestId
	c.curWorkRequestStatus = "pending"
	c.curWorkRequestDetails = "Initial cut"
	c.curHostAction = hostActionRunning
	c.curWorkItemsInABatch = res.WorkItem

	// now we need to have the task status reset
	c.curTaskStatusUpdatesForTheBatch = make([]controllerClient.TaskStatusUpdate, 0)
	for _, v := range c.curWorkItemsInABatch {
		//gather input info
		inputs := make([]controllerClient.IoStatus, 0)
		outputs := make([]controllerClient.IoStatus, 0)

		for _, io := range v.TaskIOs {
			if io.IoType == "input" {
				inputs = append(inputs, controllerClient.IoStatus{Id: io.Id})
			} else if io.IoType == "output" {
				outputs = append(outputs, controllerClient.IoStatus{Id: io.Id})
			}
		}
		c.curTaskStatusUpdatesForTheBatch = append(c.curTaskStatusUpdatesForTheBatch,
			controllerClient.TaskStatusUpdate{
				WorkRequestId:  c.curWorkRequestId,
				TaskStatus:     "waiting", // TODO what to do with a newly acquired batch?
				InternalJobId:  v.InternalJobId,
				InternalTaskId: v.InternalTaskId,
				TaskRouteId:    v.TaskRouteId,
				EngineId:       v.EngineId,
				PriorTimestamp: time.Now().Unix(),
				Timestamp:      time.Now().Unix(),
				Inputs:         inputs,
				Outputs:        outputs,
			})
	}
	return false, false, len(c.curWorkItemsInABatch), nil
}

func (c *ControllerUniverse) Terminate() {
	_, err:=c.controllerAPIClient.EngineApi.TerminateEngineInstance(
		context.WithValue(context.Background(), controllerClient.ContextAccessToken,
			c.engineInstanceRegistrationInfo.EngineInstanceToken),
		c.engineInstanceId, &controllerClient.TerminateEngineInstanceOpts{
			XCorrelationId: optional.NewInterface(c.correlationId),
		})
	log.Printf("[ControllerUniverse.Terminate:%s] TERMINATED, err=%v", c.engineInstanceId, err)
}
func (c *ControllerUniverse) updateTaskStatus(index int, status string) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()
	c.curTaskStatusUpdatesForTheBatch[index].TaskStatus = status
}

// TODO start the heart beat for the task
// Heartbeat -- could have the info = the engine instance status update for the task
func (c *ControllerUniverse) startHeartbeat(ctx context.Context, item *controllerClient.EngineInstanceWorkItem) {
	// placeholder
	log.Println("TODO TODO TODO HEARTBEAT FOR NON-CHUNK ENGINE")
}

// Work on the index-th item of the currentWorkItemsInABatch
func (c *ControllerUniverse) Work(ctx context.Context, index int) {
	curWorkItem := &c.curWorkItemsInABatch[index]
	curStatus := &c.curTaskStatusUpdatesForTheBatch[index]
	c.updateTaskStatus(index, "running")
	method := fmt.Sprintf("[ControllerUniverse.Work:%s,wr:%s,t:%s]", c.engineInstanceId, c.curWorkRequestId,
		curWorkItem.InternalTaskId)
	// make sure we have some payload!
	payloadJSON, err := InterfaceToString(curWorkItem.TaskPayload)
	if payloadJSON == "" || err != nil {
		// an error!!!
		// should fail it -- What to do with failure!
		c.batchLock.Lock()
		curStatus.FailureReason = "Missing taskPayload"
		curStatus.ErrorCount++
		curStatus.TaskStatus = "failed"
		c.batchLock.Unlock()
		return
	}
	if curWorkItem.EngineType != "chunk" {
		// start the heartbeat back to the kafka engine_status topic .. but do we have that set up at all?
		go c.startHeartbeat(ctx, curWorkItem)
	}

	log.Printf("%s, engineId=%s", method, curWorkItem.EngineId)
	switch curWorkItem.EngineId {
	case engineIdTVRA:
		fallthrough
	case engineIdWSA:
		adapter, err := adapter.NewAdaptor(payloadJSON, c.engineInstanceId,
			curWorkItem, curStatus,
			c.controllerConfig.GraphQLTimeoutDuration,
			&c.batchLock)
		var errReason worker.ErrorReason
		if err == nil {
			errReason = adapter.Run()
		}
		if errReason.Err != nil {
			// print stuff
			log.Printf("%s, Failed to run, err=%v", method, err)
			// Set TaskStatus accordingly.
			// Status would be failed -- possible for stream engines since they are in control of
			// the entire processing for the task
			c.batchLock.Lock()
			curStatus.FailureReason = string(errReason.FailureReason)
			curStatus.ErrorCount++
			curStatus.TaskStatus = "failed"
			c.batchLock.Unlock()
		}

	case engineIdSI2:
		si, err := siv2.NewStreamIngestor(payloadJSON, c.engineInstanceId,
			curWorkItem, curStatus,
			c.controllerConfig.GraphQLTimeoutDuration,
			&c.batchLock)
		var errReason worker.ErrorReason
		if err == nil {
			errReason = si.Run()
		}
		if errReason.Err != nil {
			// print stuff
			log.Printf("%s, Failed to run, err=%v", method, err)
			c.batchLock.Lock()
			curStatus.FailureReason = string(errReason.FailureReason)
			curStatus.ErrorCount++
			if getInputMode(curWorkItem) != "chunk" {
				curStatus.TaskStatus = "failed"
			}
			c.batchLock.Unlock()
		}

	default:
		panic("TO BE IMPLEMENTED")
	}
}

func getInputMode(w *controllerClient.EngineInstanceWorkItem) string {
	ios := w.TaskIOs
	for _, anIo := range ios {
		if anIo.IoType == "input" {
			return anIo.IoMode
		}
	}
	return ""
}
