package scfsio

import (
	controllerClient "github.com/veritone/realtime/modules/controller/client"
	"github.com/veritone/realtime/modules/scfs"

	"encoding/json"
	"fmt"
	"github.com/veritone/realtime/modules/logger"
	"time"
)

// preparing input and output
type LocalIOStruct struct {
	id        string
	ScfsIO    scfs.IO
	IoMode    string
	IoType    string
	IoOptions map[string]interface{}
}

func (l *LocalIOStruct) String() string {
	return l.id
}

type jobDefinition struct {
	Id    string        `json:"id"`
	Tasks []minTaskInfo `json:"tasks"`
}
type minRuntimePayload struct {
	RecordStartTime string `json:"recordStartTime"`
	RecordEndTime   string `json:"recordEndTime"`
}
type minTaskInfo struct {
	Id             string            `json:"id"`
	EngineId       string            `json:"engineId"`
	RuntimePayload minRuntimePayload `json:"runtimePayload"`
}

func GetMediaStartTime(workItem *controllerClient.EngineInstanceWorkItem) (startTime time.Time) {
	sOrg := fmt.Sprintf("%d", workItem.OrganizationId)
	scfsCache := scfs.NewCache("/", sOrg, logger.NewLogger())

	scfsOrg, err := scfsCache.GetOrg(sOrg)
	if err != nil {
		return
	}
	scfsJob, err := scfsOrg.GetJob(workItem.JobId)
	if err != nil {
		return
	}
	if sJobDef, err := scfsJob.GetDefinition(); err == nil {
		jd := &jobDefinition{}
		if err := json.Unmarshal([]byte(sJobDef), jd); err == nil {
			if jd != nil {
				for _, task := range jd.Tasks {
					if task.RuntimePayload.RecordStartTime != "" {
						t, err := time.Parse(time.RFC3339, task.RuntimePayload.RecordStartTime)
						if err == nil {
							return t
						}
						break
					}
				}
			}
		}
	}
	return
}
func GetIOForWorkItem(workItem *controllerClient.EngineInstanceWorkItem, ioType string) (res []LocalIOStruct, err error) {
	res = make([]LocalIOStruct, 0)
	for _, anIO := range workItem.TaskIOs {
		if anIO.IoType == ioType {
			// let's grab it
			var ioChildInputs = make([]scfs.ChildTaskWithInputId, 0)
			if anIO.InputFolders != nil {
				// let's get
				for _, aChildInput := range anIO.InputFolders {
					ioChildInputs = append(ioChildInputs, scfs.ChildTaskWithInputId{aChildInput.TaskId, aChildInput.Id})
				}
			}
			//	func GetIO(logger logger.Logger, orgId string, jobId string, taskId string, ioId string, ioChildInputs []ChildTaskWithInputId) (IO, error) {
			var scfsIO scfs.IO
			scfsIO, err = scfs.GetIO(logger.NewLogger(), fmt.Sprintf("%d", workItem.OrganizationId),
				workItem.JobId, workItem.TaskId, anIO.Id, ioChildInputs)

			id := fmt.Sprintf("[IO] type:%s,mode=%s, anIOId=%s, scfsioPath=%s", anIO.IoType, anIO.IoMode, scfsIO.GetPath())
			aLocalStruct := LocalIOStruct{
				id:        id,
				IoType:    ioType,
				IoMode:    anIO.IoMode,
				IoOptions: anIO.Options,
				ScfsIO:    scfsIO,
			}
			res = append(res, aLocalStruct)
		}
	}
	return
}
