package main

import (
	"context"
)

/**
runViaController:
        In a loop fetching work from controller until TTL expired or told to terminate
                For each task Item:
                        TODO - If a stream engine task:
                        If engineId is internally managed, eg. TVR, WSA or SI2 -->
                                Adapter: set up the payload.json and  the ENV as needed by the adapters -- typical: no input
                                        Let the adapter does it  usual job --> ingest from its source then write to FS (no Kafka)
                            SI2: Possible input/output:
                                                ** Stream --> SI2 --> TDO assets such as playback segment, primary media
                                                ** Stream --> SI2 --> Stream, with ffmpeg e,g, transcoding only -- NOT RECOMMENDED but should be supported
                                                ** Stream --> SI2 --> Chunks : audio, video, frame, some custome ffmpeg to segments
                                                ** Chunk --> SI2 --> Chunks    (transcoder)
                                For other engines:  Call the engine's Process webhook + callback URL from ET to allow async processing and status updates
                                        ** Chunk: If processing time is within say 5minutes (configurable) --> response received on the process Webhook call
                            Else:  They should return ACK +

                                        ** Stream/Batch: similarly

*/

func (e *Engine) runViaController(ctx context.Context) error {
	return e.controller.Run(ctx)
}