package api

import (
	"fmt"
	"sync"
	"time"
)

const (
	AssetTypeMedia           string    = "media"
	AssetTypeMediaSegment    string    = "media-segment"
	AssetTypeInitSegment     string    = "media-init"
	AssetTypeThumbnail       string    = "thumbnail"
	RecordingStatusError     TDOStatus = "error"
	RecordingStatusRecording TDOStatus = "recording"
	RecordingStatusRecorded  TDOStatus = "recorded"

	vProgramKey = "veritoneProgram"
)

type TDOStatus string

type Asset struct {
	ID          string                 `json:"id,omitempty"`
	URI         string                 `json:"uri,omitempty"`
	SignedURI   string                 `json:"signedUri,omitempty"`
	ContainerID string                 `json:"containerId,omitempty"`
	Type        string                 `json:"type,omitempty"`
	ContentType string                 `json:"contentType,omitempty"`
	Name        string                 `json:"name,omitempty"`
	Size        int64                  `json:"size,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

type TDO struct {
	ID            string                 `json:"id,omitempty"`
	Name          string                 `json:"name,omitempty"`
	Status        TDOStatus              `json:"status,omitempty"`
	Details       map[string]interface{} `json:"details,omitempty"`
	ThumbnailURL  string                 `json:"thumbnailUrl,omitempty"`
	StartDateTime time.Time              `json:"startDateTime,omitempty"`
	StopDateTime  time.Time              `json:"stopDateTime,omitempty"`
	PrimaryAsset  TDOPrimaryAsset        `json:"primaryAsset,omitempty"`
	sync.Mutex
}

func NewTDO(name string) TDO {
	return TDO{
		Name:          name,
		Status:        RecordingStatusRecording,
		StartDateTime: time.Now(),
		StopDateTime:  time.Now(),
	}
}

func (t *TDO) SetThumbnailURL(url string) error {
	defer t.Unlock()
	t.Lock()

	// workaround for setting thumbnail image
	var vProgram map[string]interface{}

	if vp, ok := t.Details[vProgramKey]; ok {
		if vProgram, ok = vp.(map[string]interface{}); !ok {
			return fmt.Errorf("%q exists in TDO details but is of type %T", vProgramKey, vp)
		}
	} else {
		vProgram = make(map[string]interface{})
		t.Details[vProgramKey] = vProgram
	}

	vProgram["programLiveImage"] = url
	t.ThumbnailURL = url

	return nil
}

type TDOFileDetails struct {
	Name      string `json:"filename,omitempty"`
	MimeType  string `json:"mimetype,omitempty"`
	Size      int    `json:"size,omitempty"`
	Segmented bool   `json:"segmented,omitempty"`
}

type TDOPrimaryAsset struct {
	ID        string `json:"id,omitempty"`
	AssetType string `json:"assetType,omitempty"`
	URI       string `json:"uri,omitempty"`
}

type Job struct {
	JobID string `json:"id"`
}
