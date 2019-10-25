package controller

import (
	"context"
	"fmt"
	"github.com/antihax/optional"
	"github.com/pkg/errors"
	"github.com/veritone/engine-toolkit/engine/processing"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"github.com/veritone/realtime/modules/engines"
	util "github.com/veritone/realtime/modules/engines/scfsio"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

func excludeThis(variable string) bool {
	switch variable {
	case "LD_LIBRARY_PATH":
		return true
	case "LS_COLORS":
		return true
	case "TERM":
		return true
	case "PWD":
		return true
	case "_":
		return true
	case "PATH":
		return true
	}
	return false
}

func getOsEnvironmentMap() map[string]interface{} {
	res := make(map[string]interface{})
	for _, envString := range os.Environ() {
		// envString:  name=value
		firstEqual := strings.Index(envString, "=")
		if firstEqual <= 0 {
			continue
		}
		name := envString[0:firstEqual]
		if !excludeThis(name) {
			res[name] = envString[firstEqual+1:]
		}
	}
	return res
}
func NewControllerUniverse(controllerConfig *VeritoneControllerConfig, etVersion, etBuildTime, etBuildTag string) (*ControllerUniverse, error) {
	engineToolkitBuildLabel := fmt.Sprintf("Veritone Engine Toolkit:%s-%s,%s", etVersion, etBuildTag, etBuildTime)

	cfg := controllerClient.Configuration{
		BasePath:      controllerConfig.ControllerUrl,
		DefaultHeader: make(map[string]string),
		UserAgent:     engineToolkitBuildLabel,
		// TODO bring your own httpClient to set your own timeout:	HTTPClient: xxxx
	}
	controllerApiClient := controllerClient.NewAPIClient(&cfg)
	containerStatus := controllerClient.ContainerStatus{}
	containerStatus.ContainerId, containerStatus.LaunchTimestamp = getInitialContainerStatus()
	correlationId := fmt.Sprintf("EngineToolkit_Host:%s,ContainerId:%s", controllerConfig.HostId, containerStatus.ContainerId)
	engineInstanceInfo := controllerClient.EngineInstanceInfo{
		LaunchId:                 controllerConfig.LaunchId,
		EngineId:                 getEnvOrGenGuid("ENGINE_ID", "", true),
		BuildLabel:               engineToolkitBuildLabel,
		EngineToolkitVersion:     etVersion,
		HostId:                   controllerConfig.HostId,
		StartupTimestamp:         time.Now().Unix(),
		DockerContainerID:        containerStatus.ContainerId,
		RuntimeExpirationSeconds: controllerConfig.ProcessingTTLInSeconds,
		LicenseExpirationSeconds: controllerConfig.LicenseExpirationInSeconds,
		LaunchEnvVariables:       getOsEnvironmentMap(),
		LaunchStatus:             "active",
		LaunchStatusInfo:         "OK",
	}
	headerOpts := &controllerClient.RegisterEngineInstanceOpts{
		XCorrelationId: optional.NewInterface(correlationId)}

	ctx := context.Background()
	log.Println("Registering with Controller, url, ", controllerConfig.ControllerMode, ", instanceInfo=", util.ToPlainString(engineInstanceInfo))
	engineInstanceRegistrationInfo, resp, err := controllerApiClient.EngineApi.RegisterEngineInstance(
		context.WithValue(ctx, controllerClient.ContextAccessToken, controllerConfig.Token),
		engineInstanceInfo,
		headerOpts)

	if resp.StatusCode >= 400 {
		log.Fatalf("Failed to register engine instance, Status: %d: %v", resp.StatusCode, extractMeaningfulHttpResponseError(err))
	}

	var producer processing.Producer
	if !controllerConfig.SkipOutputToKafka {
		var kafkaErr error
		if producer, kafkaErr = processing.NewKafkaProducer(controllerConfig.Kafka.Brokers); kafkaErr != nil {
			// just ignore for now
			log.Printf("IGNORE Failure to connect to Kafka for publishing results, err=%v", kafkaErr)
		}
	}
	if err == nil {
		log.Println("Registering response: ", util.ToPlainString(engineInstanceRegistrationInfo))
		return &ControllerUniverse{
			universeStartTime:              time.Now().Unix(),
			controllerConfig:               controllerConfig,
			controllerAPIClient:            controllerApiClient,
			engineInstanceId:               engineInstanceRegistrationInfo.EngineInstanceId,
			correlationId:                  fmt.Sprintf("ET:%s", engineInstanceRegistrationInfo.EngineInstanceId),
			engineInstanceInfo:             engineInstanceInfo,
			engineInstanceRegistrationInfo: engineInstanceRegistrationInfo,
			requestWorkForEngineIds:        discoverEngines(),
			curContainerStatus:             containerStatus,
			curHostAction:                  hostActionRunning,
			curEngineMode:                  engineModeIdle,
			producer:                       producer,
		}, nil
	}
	return nil, errors.Wrapf(err, "Failed to register engine instance with controller at %s", controllerConfig.ControllerUrl)
}

func (c *ControllerUniverse) GetTTL() int32 {
	if c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds == 0 {
		c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds = c.controllerConfig.ProcessingTTLInSeconds
	}
	log.Printf("........... LOOK HERE (REMOVE ME LATER TOO) ...... TTL=%d seconds", c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds)
	return c.engineInstanceRegistrationInfo.RuntimeExpirationSeconds
}

func (c *ControllerUniverse) SetWorkRequestStatus(id, status, details string) {
	c.batchLock.Lock()
	defer c.batchLock.Unlock()
	if id != "same" {
		c.curWorkRequestId = id
		if id == "" {
			c.curEngineMode = engineModeIdle
		} else {
			c.curEngineMode = engineModeProcessing
		}
	}
	if status != "same" {
		c.curWorkRequestStatus = status
	}
	if details != "same" {
		c.curWorkRequestDetails = details
	}
}

// =================================

// pick up value from env or generate a GUID for it
// TODO error handling if env is not defined.
func getEnvOrGenGuid(envName string, defaultValue string, required bool) (res string) {
	res = os.Getenv("ENGINE_ID")
	if required {
		return res // regardless of default value, must return, error check later
	}
	if res == "" {
		if defaultValue != "" {
			res = defaultValue
		} else {
			res = util.GenerateUuid()
		}
	}
	return res
}

/**
TODO
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
func getInitialContainerStatus() (containerId string, timestamp int64) {
	timestamp = time.Now().Unix()
	containerId = "dockerid:" + util.GenerateUuid()
	cmd := exec.Command("cat", "/proc/self/cgroup")
	out, err := cmd.CombinedOutput()
	if err == nil {
		ss := strings.Split(string(out), "\n")
		if len(ss) > 0 {
			// look for /docker
			keyword := "/docker/"
			if dockerStart := strings.Index(ss[0], keyword); dockerStart >= 0 {
				startAt := dockerStart + len(keyword)
				containerId = ss[0][startAt : startAt+16]
			}
		}
	}
	return
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
	res = append(res, engines.EngineIdTVRA)
	res = append(res, engines.EngineIdWSA)
	res = append(res, engines.EngineIdSI2Playback)
	res = append(res, engines.EngineIdSI2AssetCreator)
	res = append(res, engines.EngineIdSI2FFMPEG)
	res = append(res, engines.EngineIdOW)
	return res
}

func extractMeaningfulHttpResponseError(err error) string {
	var errorMsg = err.Error()
	if openapiErr, ok := err.(controllerClient.GenericOpenAPIError); ok {
		errorMsg = string(openapiErr.Body())
	}
	return errorMsg
}
