package controller

import (
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"sync"
	"time"
	"os"
)

const (
	engineIdSI2  = "352556c7-de07-4d55-b33f-74b1cf237f25"
	engineIdWSA  = "9e611ad7-2d3b-48f6-a51b-0a1ba40feab4"
	engineIdTVRA = "74dfd76b-472a-48f0-8395-c7e01dd7fd24"
	engineIdOW   = "8eccf9cc-6b6d-4d7d-8cb3-7ebf4950c5f3"


	hostActionTerminate = "terminate"
	hostActionRunning   = "running"

	engineModeIdle       = "idle"
	engineModeProcessing = "processing"

	workRequestActionProcess   = "process"
	workRequestActionWait      = "wait"
	workRequestActionTerminate = "terminate"
)

/*
Engine toolkit:

1. config to trigger using controller
    1. need endpoint and all the info as needed to register the engine instance when the ET starts up (see https://github.com/veritone/realtime/blob/master/modules/controller/client/docs/EngineInstanceInfo.md for this API https://github.com/veritone/realtime/blob/master/modules/controller/client/docs/EngineApi.md#registerengineinstance
    2. Note that ET can be acting on SI and adapter behalf to get work, in this case, the assumption is ET has been bundled with the adapter or SI
    3. For now, we'll just do one at a a time -- so webstream adapter will be recompiled with ET in front of it

2. when in that mode:
    1. Upon startup, ET will :
        1. Register its instance with controller api :  https://github.com/veritone/realtime/blob/master/modules/controller/client/docs/EngineApi.md#registerengineinstance
        2. in a loop
            1. get work from controller  https://github.com/veritone/realtime/blob/master/modules/controller/client/docs/EngineApi.md#getengineinstancework
            2. For each of the work items (phase 1: only 1), which has info like taskPayload, IO etc:
                1. invoke the engine's process or via webhook with the right payload & config
                2. for stream engines such as adapter
                    1. WsA will need to be modified to accept the child output as well so that the hard links can be set up for the next child process
                    2. WsA will just do its own thing..
                3. for stream engine such as SI
                    1. similarly just let the SI work by starting it with the PAYLOAD JSON, CONFIGl, ALL the env variables as needed (Where do we get them??)
                        1. However tricky is when it's producing chunks:  --> the task parent IO should have been set up for SI tasks (as parents) to pass on the right outputs to the next tasks' Inputs -- currently we'll just have SI drops the stuff into chunk_all queue ..
                4. When ET starts other kinds of engine, especially the one that produces chunks -- or chunk-results -->  2 modes:
                    1. invoke the webhook (/process) N number of times, where N comes from the unit of work as returned from the work item,
                        1. ET will need to use SCFS API to get the chunk --> handover to the webhook /process
                        2. ET will receive the results from webhook -->drop into chunk_all topics unless skipOutputToKafka is true...
                            1. Also write the chunk results using SCFS
            3. Heart beat thread to report the engine instance status -- this includes updates the parent count, the child input count, for each work item
        3. Break loop when the getWork returns a terminate or have been receiving WAIT continuously for t elapsed time , where t was given

*/

// enable via the VERITONE_CONTROLLER_CONFIG_JSON=the ControllerConfig
type VeritoneControllerConfig struct {
	ControllerMode       bool   `json:"controllerMode"`
	ControllerUrl        string `json:"controllerUrl"`
	HostId               string `json:"hostId"`
	SkipOutputToKafka    bool   `json:"skipOutputToKafka"`
	Token                string `json:"token"`
	UpdateStatusInterval string `json:"updateStatusInterval"`
	updateStatusDuration time.Duration
	// default ttl can be overriden here..
	ProcessingTTLInSeconds     int32 `json:"processingTTLInSeconds`
	LicenseExpirationInSeconds int32 `json:"licenseExpirationInSeconds"` // 0 == never expires

	// setting to tune how long to wait for new work, and sleep interval while waiting
	IdleWaitTimeoutInSeconds   int32 `json:"idleWaitTimeoutInSeconds"`
	IdleQueryIntervalInSeconds int32 `json:"idleQueryIntervalInSeconds"`

	// GraphQL
	GraphQLTimeoutDuration string `json:"graphqlTimeoutDuration"`
}

func (c *VeritoneControllerConfig) String() string {
	return ToString(c)
}
func SampleVeritoneControllerConfig () string {
	sampleConfig:=VeritoneControllerConfig{
		ControllerMode: true,
		ControllerUrl : "http://localhost:9000/edge/v1",
		HostId: GenerateUuid(),
		Token:GenerateUuid(),
		UpdateStatusInterval:"5s",

		ProcessingTTLInSeconds: 6000,
		LicenseExpirationInSeconds: 100000,
		IdleQueryIntervalInSeconds: 5,
		IdleWaitTimeoutInSeconds: 60,
	}
	return sampleConfig.String()
}
func (c *VeritoneControllerConfig) SetDefaults() {
	c.ControllerMode = true
	if c.UpdateStatusInterval == "" {
		c.UpdateStatusInterval = "5s"
	}
	var e error
	if c.updateStatusDuration, e = time.ParseDuration(c.UpdateStatusInterval); e != nil {
		c.updateStatusDuration = 5 * time.Second
	}
	if c.IdleWaitTimeoutInSeconds == 0 {
		c.IdleWaitTimeoutInSeconds = 60 // idle wait is 60 seconds if theres no work in 60 seconds
	}
	if c.IdleQueryIntervalInSeconds == 0 {
		c.IdleQueryIntervalInSeconds = 5 // between waiting for work to come in..
	}
	if c.ProcessingTTLInSeconds == 0 {
		c.ProcessingTTLInSeconds = 3600 * 2
	}
	if c.GraphQLTimeoutDuration == "" {
		c.GraphQLTimeoutDuration = "60s"
	}
	if c.HostId == "" {
		c.HostId = os.Getenv("HOST_ID")
	}
}

type ControllerUniverse struct {
	universeStartTime   int64
	controllerConfig    *VeritoneControllerConfig
	controllerAPIClient *controllerClient.APIClient

	engineInstanceId string
	correlationId    string

	engineInstanceInfo             controllerClient.EngineInstanceInfo
	engineInstanceRegistrationInfo controllerClient.EngineInstanceRegistrationInfo
	// This is the set of engine ids that this instance will get work for.  Controller will return the highest priority task across the set of engine ids
	requestWorkForEngineIds []string

	curContainerStatus controllerClient.ContainerStatus
	curHostAction      string // terminate or running
	curEngineMode      string // idle or processing

	curWorkRequestId      string
	curWorkRequestStatus  string
	curWorkRequestDetails string

	// go tightly together
	batchLock                       sync.Mutex
	curWorkItemsInABatch            []controllerClient.EngineInstanceWorkItem
	curTaskStatusUpdatesForTheBatch []controllerClient.TaskStatusUpdate

	priorTimestamp int64
}
