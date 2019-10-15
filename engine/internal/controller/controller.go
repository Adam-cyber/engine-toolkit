package controller

import (
	"context"
	"fmt"
	"github.com/antihax/optional"
	"github.com/pkg/errors"
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"log"
	"os"
	"time"
)

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
		LaunchId:                 getEnvOrGenGuid("LAUNCH_ID", "", false),
		EngineId:                 getEnvOrGenGuid("ENGINE_ID", "", true),
		BuildLabel:               engineToolkitBuildLabel,
		EngineToolkitVersion:     etVersion,
		HostId:                   controllerConfig.HostId,
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
			engineInstanceId:               engineInstanceRegistrationInfo.EngineInstanceId,
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
