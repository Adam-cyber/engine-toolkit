package controller

import (
	"context"
	"github.com/antihax/optional"
	"github.com/veritone/engine-toolkit/engine/internal/controller/adapter"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"log"
	"time"

	"fmt"
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

// simple path:  just go back to controller for work
//
func (c *ControllerUniverse) GetWorks(ctx context.Context) (done bool, waitForMore bool, nItems int, err error) {

	headerOpts := &controllerClient.GetEngineInstanceWorkOpts{
		XCorrelationId: optional.NewInterface(c.correlationId),
	}
	// by the time we're here we know a thing or two about the WorkRequsetStatus
	// and we also should have TaskStatus updated
	curEngineWorkRequest := controllerClient.EngineInstanceWorkRequest{
		WorkRequestId:     c.curWorkRequestId,
		WorkRequestStatus: c.curWorkRequestStatus,
		TaskStatus:        c.curTaskStatusUpdatesForTheBatch,
		ContainerStatus:   c.curContainerStatus,
	}
	res, _, err := c.controllerAPIClient.EngineApi.GetEngineInstanceWork(
		context.WithValue(ctx, controllerClient.ContextAccessToken,
			c.engineInstanceRegistrationInfo.EngineInstanceToken),
		c.engineInstanceId,
		curEngineWorkRequest, headerOpts)
	if err != nil {
		// would this be a failure?
		// todo errorhandling for now just log and sleep for the next batch
		log.Printf("controller.GetEngineInstanceWork returning err=%v", err)
		return false, false, 0, err
	}
	// todo logging of old vs. new
	switch res.Action {
	case workRequestActionTerminate:
		//ignoring error here
		c.controllerAPIClient.EngineApi.TerminateEngineInstance(
			context.WithValue(ctx, controllerClient.ContextAccessToken,
				c.engineInstanceRegistrationInfo.EngineInstanceToken),
			c.engineInstanceId, &controllerClient.TerminateEngineInstanceOpts{
				XCorrelationId: optional.NewInterface(c.correlationId),
			})
		// bye bye
		return true, false, 0, nil // TODO also put some thing into some channel to say that we're done?
	case workRequestActionWait:
		return false, true, 0, nil
	}
	// now we have a new batch of work
	c.batchLock.Lock()
	c.curWorkRequestId = res.WorkRequestId
	c.curWorkItemsInABatch = res.WorkItem
	c.priorTimestamp = time.Now().Unix()
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
	c.batchLock.Unlock()
	return false, false, len(c.curWorkItemsInABatch), nil
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
	curWorkItem := c.curWorkItemsInABatch[index]
	curStatus := c.curTaskStatusUpdatesForTheBatch[index]
	c.updateTaskStatus(index, "running")
	method := fmt.Sprintf("[ControllerUniverse.Work (%s:%s)]", c.curWorkRequestId,
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
		go c.startHeartbeat(ctx, &curWorkItem)
	}

	log.Printf("%s, engineId=%s", method, curWorkItem.EngineId)
	switch curWorkItem.EngineId {
	case engineIdTVRA:
		fallthrough
		// make sure they are the same
	case engineIdWSA:
		adapter, err := adapter.NewAdaptor(payloadJSON, c.engineInstanceId, &curWorkItem, &curStatus, &c.batchLock)
		if err == nil {
			err = adapter.Run()
			adapter.Close()
		}
		if err != nil {
			// print stuff
			log.Printf("%s, Failed to run, err=%v", method, err)
		}

	case engineIdSI2: // TODO
	default:
		panic("TO BE IMPLEMENTED")
	}
}
