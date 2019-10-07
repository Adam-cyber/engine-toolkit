package main

import (
	"context"
	"github.com/veritone/realtime/modules/controller/client"
	"github.com/antihax/optional"
	"fmt"
	"time"
	"os"
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
type ControllerUniverse struct {
	controllerConfig *VeritoneControllerConfig
	controllerAPIClient *client.APIClient
	token string
	engineInstanceId string
	correlationId string
	engineInstanceInfo *client.EngineInstanceInfo
	engineInstanceRegistrationInfo *client.EngineInstanceRegistrationInfo
	curEngineWorkRequest *client.EngineInstanceWorkRequest
}
func newControllerUniverse(controllerConfig *VeritoneControllerConfig) *ControllerUniverse{
	cfg := client.Configuration{
		BasePath: controllerConfig.ControllerUrl,
		DefaultHeader: make(map[string]string),
		UserAgent: "engine-toolkit:",
	}
	controllerApiClient := client.NewAPIClient(&cfg)
	correlationId := fmt.Sprintf("EngineToolkitCorrelationGoesHere TODO")

	engineInstanceInfo := client.EngineInstanceInfo {
		LaunchId : "TODO",
		EngineId : "TODO",
		BuildLabel : "TODO",
		EngineToolkitVersion: "TODO",
		HostId : "TODO",
		StartupTimestamp: time.Now().Unix(),
		DockerContainerID: "TODO",
		RuntimeExpirationSeconds: 600,  // TODO
		LicenseExpirationSeconds: 1000000000,  //TODO
		LaunchEnvVariables: map[string]interface{} {"VERITONE_CONTROLLER_CONFIG_JSON":os.Getenv("VERITONE_CONTROLLER_CONFIG_JSON")},
		LaunchStatus: "TODO PROBABLY active",
		LaunchStatusInfo:"No problem",
	}

	headerOpts := &client.RegisterEngineInstanceOpts {
		XCorrelationId:optional.NewInterface(correlationId)}

	ctx := context.Background()
	engineInstanceRegistrationInfo, _, err := controllerApiClient.EngineApi.RegisterEngineInstance(
		context.WithValue(ctx, client.ContextAccessToken, controllerConfig.Token),
		"ENGINEID _ TODO",
		engineInstanceInfo,
		headerOpts)
	if err==nil {
		return 	&ControllerUniverse{
				controllerConfig: controllerConfig,
				controllerAPIClient: controllerApiClient,
				correlationId: correlationId,
				engineInstanceInfo: &engineInstanceInfo,
				engineInstanceRegistrationInfo: &engineInstanceRegistrationInfo,
			}
	}
	return nil
}

/**
TODO fetch work
 */
func (c *ControllerUniverse) getWork(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()


	for {
		select {
		  case :
		  	default:

		headerOpts := & client.GetEngineInstanceWorkOpts{
			XCorrelationId: optional.NewInterface(c.correlationId),
		}

		res, _, err := c.controllerAPIClient.EngineApi.GetEngineInstanceWork(
			context.WithValue(ctx, client.ContextAccessToken, c.engineInstanceRegistrationInfo.EngineInstanceToken),
			c.engineInstanceRegistrationInfo.EngineInstanceId,
			*c.curEngineWorkRequest, headerOpts)


		)
		}
	}
}