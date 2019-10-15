package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os/exec"
	"time"
	"sync"
)

/**
runViaController:
	In a loop fetching work from controller until TTL expired or told to terminate
		For each task Item:
			TODO - If a stream engine task:
				Start heartbeat loop on the engine behalf until it's done?   <<<< KAFKA ALERT
			If engineId is internally managed, eg. TVR, WSA or SI2 -->
				Adapter: set up the payload.json and  the ENV as needed by the adapters -- typical: no input
					Let the adapter does it  usual job --> ingest from its source then write to FS (no Kafka)
			    SI2: Possible input/output:

						** Stream --> SI2 --> Stream for downstream stream engines:  Not Support -- no stream-buffer business

						** Stream --> SI2 --> Chunks for another SI2 engine to transcode/reencode etc.)  --> chunks (cognitive) definitely

						** Stream --> SI2 --> Chunks for other cognitive engines such as transcription, translation etc.

						** Chunk --> SI2 --> Chunks    (transcoder)

				SI: setup the input payload as in SI right now to consume from scfs
				Invoke the `entrypoints` for the adapter or SI

				SI2: will need to be modified to have just ffmpeg and can consume from stream (from adapters),
					or chunks (from another SI2 parent task)

				SI2: consuming stream producing chunks -- e.g. to audio chunks for transcription
					use scfs to consume streams and producing chunks to both scfs and Kafka (?)   <<<<< KAFKA ALERT

				SI2 in consuming chunks, producing chunks (e.g. to split into chunks for audio  will have to:
					get the chunks as if a chunk engine --> using the scfs Chunk,
                    producing chunks


		* First phase:  Adapters and SI --> any chunk output of SI should go to Kafka `chunk_all` as currently
				Stream output may not be supported

		* get input, output from scfs.Cache --> org --> job --> task --> IO

*/

func (e *Engine) runViaController(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cmd *exec.Cmd

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
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(time.Duration(e.controller.GetTTL()) * time.Second):
				e.logDebug(fmt.Sprintf("Time is up (TTL is %s) -- GETTING OUT", e.controller.GetTTL()))
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
	e.controller.Terminate(ctx)
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
	e.controller.SetWorkRequestStatus("same", "running", fmt.Sprintf("New batch %d items", batchSize))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			e.controller.SetWorkRequestStatus("same", "same", fmt.Sprintf("working on %d/%d...", processedCount, batchSize))
			e.controller.Work(ctx, processedCount)
			processedCount++ //move on to the next one..
			if processedCount == batchSize {
				// done
				e.controller.SetWorkRequestStatus("same", "complete", fmt.Sprintf("completed %d items", batchSize))
				return
			}
		}
	}
}
