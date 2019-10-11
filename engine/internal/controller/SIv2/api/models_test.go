package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTDO(t *testing.T) {
	name := "name1"
	tdo := NewTDO(name)
	assert.Equal(t, name, tdo.Name)
	assert.Equal(t, RecordingStatusRecording, tdo.Status)
	assert.False(t, tdo.StartDateTime.IsZero())
	assert.False(t, tdo.StopDateTime.IsZero())
}

func TestSetThumbnailURL(t *testing.T) {
	url := "url1"
	tdo := NewTDO("name1")
	tdo.Details = make(map[string]interface{})

	tdo.SetThumbnailURL(url)

	assert.Equal(t, url, tdo.ThumbnailURL)
	assert.Equal(t, url, tdo.Details[vProgramKey].(map[string]interface{})["programLiveImage"])
}
