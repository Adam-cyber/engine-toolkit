package controller

import (
	"context"
	"fmt"
	"github.com/antihax/optional"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	engines "github.com/veritone/realtime/modules/engines"
	"github.com/veritone/realtime/modules/engines/outputWriter"
	util "github.com/veritone/realtime/modules/engines/scfsio"
	siv2core "github.com/veritone/realtime/modules/engines/siv2core"
	siv2ffmpeg "github.com/veritone/realtime/modules/engines/siv2ffmpeg"
	siv2playback "github.com/veritone/realtime/modules/engines/siv2playback"
	"github.com/veritone/realtime/modules/engines/worker"
	wsa_tvr "github.com/veritone/realtime/modules/engines/wsa_tvr_adapter"
	"github.com/veritone/realtime/modules/logger"
	"log"
	"time"
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
		WorkRequestId:           c.curWorkRequestId,
		WorkRequestStatus:       c.curWorkRequestStatus,
		WorkRequestDetails:      c.curWorkRequestDetails,
		HostId:                  c.engineInstanceInfo.HostId,
		HostAction:              c.curHostAction,
		RequestWorkForEngineIds: c.requestWorkForEngineIds,
		TaskStatus:              c.curTaskStatusUpdatesForTheBatch,
		ContainerStatus:         c.curContainerStatus,
	}
	res, _, err := c.controllerAPIClient.EngineApi.GetEngineInstanceWork(
		context.WithValue(ctx, controllerClient.ContextAccessToken,
			c.engineInstanceRegistrationInfo.EngineInstanceToken),
		c.engineInstanceId,
		curEngineWorkRequest, headerOpts)
	if err != nil {
		// If there's an error -- probably need to retry in a few seconds
		// much like the `wait`
		realErrMsg := extractMeaningfulHttpResponseError(err)
		log.Printf("%s returning err=%s", method, realErrMsg)
		return false, false, 0, fmt.Errorf("Failed to get work from controller, err=%s", realErrMsg)
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
	_, err := c.controllerAPIClient.EngineApi.TerminateEngineInstance(
		context.WithValue(context.Background(), controllerClient.ContextAccessToken,
			c.engineInstanceRegistrationInfo.EngineInstanceToken),
		c.engineInstanceId, &controllerClient.TerminateEngineInstanceOpts{
			XCorrelationId: optional.NewInterface(c.correlationId),
		})
	terminatedStatus := "OK"
	if err != nil {
		terminatedStatus = extractMeaningfulHttpResponseError(err)
	}
	log.Printf("[ControllerUniverse.Terminate:%s] TERMINATED terminated status=%s", c.engineInstanceId, terminatedStatus)
}
func (c *ControllerUniverse) updateTaskStatus(index int, status string) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()
	c.curTaskStatusUpdatesForTheBatch[index].TaskStatus = status
}

// Work on the index-th item of the currentWorkItemsInABatch
func (c *ControllerUniverse) Work(ctx context.Context, index int) {
	curWorkItem := &c.curWorkItemsInABatch[index]
	curStatus := &c.curTaskStatusUpdatesForTheBatch[index]
	c.updateTaskStatus(index, "running")
	method := fmt.Sprintf("[ControllerUniverse.Work:%s,wr:%s,t:%s]", c.engineInstanceId, c.curWorkRequestId,
		curWorkItem.InternalTaskId)
	// make sure we have some payload!
	var (
		wrk                      worker.Worker
		workItemStatusManager, _ = util.GetWorkItemStatusManager(curStatus, &c.batchLock)
		inputIOs                 []util.LocalSCFSIOInput
		outputIOs                []util.LocalSCFSIOOutput
		inputErr, outputErr      error
	)
	log.Printf("%s, engineId=%s", method, curWorkItem.EngineId)
	payloadJSON, err := util.InterfaceToString(curWorkItem.TaskPayload)
	if payloadJSON == "" || err != nil {
		workItemStatusManager.ReportError(true, nil, curWorkItem.InternalTaskId, "Internal", "Missing taskPayload")
		return
	}

	var inputOutputStatus string
	if inputIOs, inputErr = util.GetIOInputsForWorkItem(curWorkItem, c.engineInstanceId); inputErr != nil {
		inputOutputStatus = fmt.Sprintf("Input IO Error:%s\n", inputErr.Error())
		log.Printf("%s ERROR: Failed to get IO Input for workItem, jobId=%s, taskId=%s, err=%v", method, curWorkItem.JobId, curWorkItem.TaskId, inputErr)
	}
	if outputIOs, outputErr = util.GetIOOutputsForWorkItem(curWorkItem, c.engineInstanceId); outputErr != nil {
		inputOutputStatus = fmt.Sprintf("Output IO Error:%s\n", outputErr.Error())
		log.Printf("%s ERROR: Failed to get IO Output for workItem, jobId=%s, taskId=%s, err=%v", method, curWorkItem.JobId, curWorkItem.TaskId, outputErr)
	}
	// exit out of here..
	if len(inputOutputStatus) > 0 {
		workItemStatusManager.ReportError(true, nil, curWorkItem.InternalTaskId, "Internal", inputOutputStatus)
		return
	}

	switch curWorkItem.EngineId {
	case engines.EngineIdTVRA:
		fallthrough
	case engines.EngineIdWSA:
		wrk, err = wsa_tvr.NewAdapter(payloadJSON,
			c.engineInstanceId,
			curWorkItem,
			c.controllerConfig.GraphQLTimeoutDuration,
			c.controllerConfig.ControllerUrl,
			workItemStatusManager,
			inputIOs, outputIOs)

	case engines.EngineIdSI2Playback:
		wrk, err = siv2playback.NewSI2Playback(payloadJSON,
			c.engineInstanceId,
			curWorkItem,
			c.controllerConfig.GraphQLTimeoutDuration,
			c.controllerConfig.ControllerUrl,
			workItemStatusManager,
			inputIOs, outputIOs)

	case engines.EngineIdSI2AssetCreator:
		wrk, err = siv2core.NewSIV2Core(payloadJSON,
			c.engineInstanceId,
			curWorkItem,
			c.controllerConfig.GraphQLTimeoutDuration,
			c.controllerConfig.ControllerUrl,
			workItemStatusManager,
			inputIOs, outputIOs)

	case engines.EngineIdSI2FFMPEG:
		wrk, err = siv2ffmpeg.NewSIV2FFMPEG(payloadJSON,
			c.engineInstanceId,
			curWorkItem,
			c.controllerConfig.GraphQLTimeoutDuration,
			c.controllerConfig.ControllerUrl,
			workItemStatusManager,
			inputIOs, outputIOs)

	case engineIdOW:
		wrk, err = outputwriter.NewOutputWriter(payloadJSON, &c.batchLock, curWorkItem, curStatus, logger.NewLogger())

	default:
		/*
			 TODO for other chunk engines:
				* need to read from the task's input IO
			    * Acquire a chunk
			    	* read the `media-chunk` from the userMetadata for the chunk
			        * handling similar to that of the `runInference`.
			        * However for the output --> we'll need to write to the output, if provided AND chunk_all
			           Also some house keeping:  increment the processedCount for both input/output
		*/
		panic("TO BE IMPLEMENTED")
	}

	var errReason worker.ErrorReason
	if err == nil {
		errReason = wrk.Run(ctx)
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
		if getInputMode(curWorkItem) != "chunk" {
			curStatus.TaskStatus = "failed"
		}
		c.batchLock.Unlock()
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
