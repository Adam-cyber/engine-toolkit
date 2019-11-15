package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os/exec"
	"sync"
	"time"
	"go.uber.org/atomic"
	"github.com/veritone/realtime/modules/controller/client"
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cmd *exec.Cmd

	log.Printf("Log will be written to %s", e.Config.ControllerConfig.LogFileName)

	// Future migration of other engines --> so keep here
	if len(e.Config.Subprocess.Arguments) > 0 {
		cmd = exec.CommandContext(ctx, e.Config.Subprocess.Arguments[0], e.Config.Subprocess.Arguments[1:]...)
		cmd.Stdout = e.Config.Stdout
		cmd.Stderr = e.Config.Stderr
		if err := cmd.Start(); err != nil {
			return errors.Wrap(err, e.Config.Subprocess.Arguments[0])
		}
		readyCtx, cancel := context.WithTimeout(ctx, e.Config.Subprocess.ReadyTimeout)
		defer cancel()
		e.logDebug("waiting for ready... will expire after", e.Config.Subprocess.ReadyTimeout)
		if err := e.ready(readyCtx); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go e.controller.UpdateEngineInstanceStatus(ctx, &wg)
	// simple loop to get the work
	var waitElapsedInSeconds int32

	wg.Add(1)
	ttlTimer := time.NewTimer(time.Duration(e.controller.GetTTL()) * time.Second)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ttlTimer.C:
				e.logDebug("Time is up (TTL is %d) -- GETTING OUT", e.controller.GetTTL())
				cancel()
				return
			case <-ctx.Done():
				return
			default:
				done, waitForMore, batchSize, err := e.controller.AskForWork(ctx)
				if done {
					return
				}
				if waitForMore || err != nil {
					e.controller.SetWorkRequestStatus("", "", "")
					if waitElapsedInSeconds > e.Config.ControllerConfig.IdleWaitTimeoutInSeconds {
						return
					}
					waitElapsedInSeconds += e.Config.ControllerConfig.IdleQueryIntervalInSeconds
					time.Sleep(time.Duration(e.Config.ControllerConfig.IdleQueryIntervalInSeconds) * time.Second)
					continue
				} else {
					//reset
					waitElapsedInSeconds = 0
				}
				e.processWorkRequest(ctx, batchSize)
			}
		}
	}()
	wg.Wait()
	if cmd != nil {
		// wait for the command
		if err := cmd.Wait(); err != nil {
			if err := ctx.Err(); err != nil {
				// if the context has an error, we'll assume this command
				// errored because we terminated it (via context).
				return ctx.Err()
			}
			// otherwise, the subprocess has crashed
			return errors.Wrap(err, e.Config.Subprocess.Arguments[0])
		}
		return nil
	}
	// wait for all done?
	<-ctx.Done()

	// one more ... tell Controller that we're terminated
	e.controller.Terminate()
	// close up shop?
	return nil
}

/** a simple process batch job */

func (e *Engine) processWorkRequest(ctx context.Context, batchSize int) {
	// here we have N items, we need to iterate thru each one
	// Forget about concurrency for now.
	// Do care about timeout
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	processedCount := 0
	e.controller.SetWorkRequestStatus("", client.WorkRequestStatusEnum_RUNNING, fmt.Sprintf("New batch %d items", batchSize))
	var engineIsWorking atomic.Bool
	for {
		select {
		case <-ctx.Done():
			// not when we're working on something..
			for {
				if engineIsWorking.Load() {
					e.logDebug("Engine is still working .. sleep a bit")
					time.Sleep(500*time.Millisecond)
				}
			}
			return
		default:
			e.controller.SetWorkRequestStatus("", "", fmt.Sprintf("working on %d/%d...", processedCount+1, batchSize))
			engineIsWorking.Store(true)
			e.controller.Work(ctx, processedCount)
			engineIsWorking.Store(false)
			processedCount++ //move on to the next one..
			if processedCount == batchSize {
				// done
				e.controller.SetWorkRequestStatus("", client.WorkRequestStatusEnum_COMPLETE, fmt.Sprintf("completed %d items", batchSize))
				return
			}
		}
	}
}
