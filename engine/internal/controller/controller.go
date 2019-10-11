package controller

import (
	"context"
	"fmt"
	"github.com/antihax/optional"
	"github.com/pkg/errors"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"log"
	"os"
	"sync"
	"time"
)

const (
	engineIdSI2  = "352556c7-de07-4d55-b33f-74b1cf237f25"
	engineIdWSA  = "9e611ad7-2d3b-48f6-a51b-0a1ba40feab4"
	engineIdTVRA = "74dfd76b-472a-48f0-8395-c7e01dd7fd24"

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
type ManagedEngineInfo struct {
	EngineId      string `json:"engineId"`
	EngineMode    string `json:"engineMode"` // batch, stream, chunk
	EngineCmdLine string `json:"engineCmdLine"`
}
type VeritoneControllerConfig struct {
	ControllerMode       bool   `json:"controllerMode"`
	ControllerUrl        string `json:"controllerUrl"`
	HostId               string `json:"hostId"`
	SkipOutputToKafka    bool   `json:"skipOutputToKafka"`
	Token                string `json:"token"`
	UpdateStatusInterval string `json:"updateStatusInterval"`
	updateStatusDuration time.Duration
	// the map identify the engines that can be managed within this engine toolkit instance
	ManagedEngines []ManagedEngineInfo `json:"managedEngines"`

	ProcessingTTLInSeconds int32 `json:"processingTTLInSeconds`
	LicenseExpirationInSeconds int32 `json:"licenseExpirationInSeconds"`  // 0 == never expires
	// Other
	IdleWaitTimeoutInSeconds int32 `json:"idleWaitTimeoutInSeconds"`
	IdleQueryIntervalInSeconds int32 `json:"idleQueryIntervalInSeconds"`

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
	if c.ProcessingTTLInSeconds == 0{
		c.ProcessingTTLInSeconds = 3600 * 2
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

	curWorkRequestId     string
	curWorkRequestStatus string

	// go tightly together
	batchLock                       sync.Mutex
	curWorkItemsInABatch            []controllerClient.EngineInstanceWorkItem
	curTaskStatusUpdatesForTheBatch []controllerClient.TaskStatusUpdate

	priorTimestamp int64
}

// pick up value from env or generate a GUID for it
// TODO error handling if env is not defined.
func GetEnvOrGenGuid(envName string, defaultValue string, required bool) (res string) {
	res = os.Getenv("ENGINE_ID")
	if required {
		return res // regardless of default value, must return, error check later
	}
	if res == "" {
		if defaultValue != "" {
			res = defaultValue
		} else {
			res = GenerateUuid()
		}
	}
	return res
}

/**
get the container id by
cat /proc/self/cgroup
13:name=systemd:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
12:pids:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
11:hugetlb:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
10:net_prio:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
9:perf_event:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
8:net_cls:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
7:freezer:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
6:devices:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
5:memory:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
4:blkio:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
3:cpuacct:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
2:cpu:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
1:cpuset:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc

the rest -- just fab
*/
func getInitialContainerStatus() (status controllerClient.ContainerStatus) {
	status.ContainerId = "dockerid:" + GenerateUuid()
	status.LaunchTimestamp = time.Now().Unix()
	return
}

func NewControllerUniverse(controllerConfig *VeritoneControllerConfig, engineToolkitBuildTag string) (*ControllerUniverse, error) {
	engineToolkitVersion := fmt.Sprintf("Veritone Engine Toolkit:%s", engineToolkitBuildTag)
	hostID := GetEnvOrGenGuid("HOST_ID", "", false)
	cfg := controllerClient.Configuration{
		BasePath:      controllerConfig.ControllerUrl,
		DefaultHeader: make(map[string]string),
		UserAgent:     fmt.Sprintf("engine-toolkit:%s", engineToolkitVersion),
	}
	controllerApiClient := controllerClient.NewAPIClient(&cfg)
	correlationId := fmt.Sprintf("ETREG_FROM_HOST_ID:%s", hostID)
	containerStatus := getInitialContainerStatus()
	engineInstanceInfo := controllerClient.EngineInstanceInfo{
		LaunchId:                 GetEnvOrGenGuid("LAUNCH_ID", "", false),
		EngineId:                 GetEnvOrGenGuid("ENGINE_ID", "", true),
		BuildLabel:               GetEnvOrGenGuid("BUILD_LABEL", "TODO _ SETUP BUILDLABEL", false),
		EngineToolkitVersion:     engineToolkitVersion,
		HostId:                   hostID,
		StartupTimestamp:         time.Now().Unix(),
		DockerContainerID:        containerStatus.ContainerId,
		RuntimeExpirationSeconds: controllerConfig.ProcessingTTLInSeconds,
		LicenseExpirationSeconds: controllerConfig.LicenseExpirationInSeconds,
		LaunchEnvVariables:       map[string]interface{}{"VERITONE_CONTROLLER_CONFIG_JSON": os.Getenv("VERITONE_CONTROLLER_CONFIG_JSON")},
		LaunchStatus:             "active",
		LaunchStatusInfo:         "OK",
	}

	headerOpts := &controllerClient.RegisterEngineInstanceOpts{
		XCorrelationId: optional.NewInterface(correlationId)}

	ctx := context.Background()
	log.Println("Registering with Controller, url, ", controllerConfig.ControllerMode, ", instanceInfo=", ToPlainString(engineInstanceInfo))
	engineInstanceRegistrationInfo, _, err := controllerApiClient.EngineApi.RegisterEngineInstance(
		context.WithValue(ctx, controllerClient.ContextAccessToken, controllerConfig.Token),
		engineInstanceInfo,
		headerOpts)
	if err == nil {
		log.Println("Registering response: ", ToPlainString(engineInstanceRegistrationInfo))
		return &ControllerUniverse{
			universeStartTime:              time.Now().Unix(),
			controllerConfig:               controllerConfig,
			controllerAPIClient:            controllerApiClient,
			correlationId:                  fmt.Sprintf("ET:%s", engineInstanceRegistrationInfo.EngineInstanceId),
			engineInstanceInfo:             engineInstanceInfo,
			engineInstanceRegistrationInfo: engineInstanceRegistrationInfo,
			requestWorkForEngineIds:        discoverEngines(),
			curContainerStatus:             containerStatus,
			curHostAction:                  hostActionRunning,
			curEngineMode:                  engineModeIdle,
		}, nil
	}
	return nil, errors.Wrapf(err, "Failed to register engine instance with controller at %s", controllerConfig.ControllerUrl)
}

// get the request Work for engine ids
// this is a bit tricky in that if we want to have adapters and SI, we need say ffmpeg, streamlink, python etc
//

func discoverEngines() []string {
	// the first one is the ENGINE_ID env variable
	res := make([]string, 0)
	if mainEngineId := os.Getenv("ENGINE_ID"); mainEngineId != "" {
		res = append(res, mainEngineId)
	}
	// TODO -- need to really check for ffmpeg, streamlink as required by adapters, si
	// or now we'll just blindly think that it's there
	res = append(res, engineIdTVRA)
	res = append(res, engineIdWSA)
	res = append(res, engineIdSI2)
	return res
}

func (c *ControllerUniverse) GetTTL() int32 {
	if c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds == 0 {
		c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds = c.controllerConfig.ProcessingTTLInSeconds
	}
	return c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds
}
