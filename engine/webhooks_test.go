package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/matryer/is"
)

func TestNewRequestFromMediaChunk(t *testing.T) {
	is := is.New(t)
	fileSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.WriteString(w, "this simulates data")
		is.NoErr(err)
	}))
	defer fileSrv.Close()
	msg := mediaChunkMessage{
		CacheURI:      fileSrv.URL,
		MIMEType:      "application/test",
		ChunkIndex:    1,
		ChunkUUID:     "chunkUUID",
		JobID:         "jobID",
		TaskID:        "taskID",
		TDOID:         "tdoID",
		StartOffsetMS: 1000,
		EndOffsetMS:   2000,
		Width:         500,
		Height:        600,
		Content:       "content",
		TaskPayload: []byte(`{
			"libraryID":            "lib123",
			"libraryEngineModelID": "libraryEngineModel123",
			"veritoneAPIBaseURL":   "https://test.veritone.com/api",
			"token":                "tok123",
			"customField1": 		"abc123"
		}`),
	}
	client := http.DefaultClient
	var wg sync.WaitGroup
	wg.Add(1)
	processSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		is.Equal(r.FormValue("chunkMimeType"), "application/test")
		is.Equal(r.FormValue("chunkIndex"), "1")
		is.Equal("chunkUUID", r.FormValue("chunkUUID"))
		//is.Equal("jobID", r.FormValue("jobID"))
		//is.Equal("taskID", r.FormValue("taskID"))
		//is.Equal("tdoID", r.FormValue("tdoID"))
		is.Equal(r.FormValue("startOffsetMS"), "1000")
		is.Equal(r.FormValue("endOffsetMS"), "2000")
		is.Equal(r.FormValue("width"), "500")
		is.Equal(r.FormValue("height"), "600")
		is.Equal(r.FormValue("libraryId"), "lib123")
		is.Equal(r.FormValue("libraryEngineModelId"), "libraryEngineModel123")
		is.Equal(r.FormValue("cacheURI"), fileSrv.URL)
		is.Equal(r.FormValue("veritoneApiBaseUrl"), "https://test.veritone.com/api")
		is.Equal(r.FormValue("token"), "tok123")
		//is.Equal("content", r.FormValue("content"))

		is.True(r.FormValue("payload") != "")
		// check the payload
		var payload map[string]interface{}
		err := json.Unmarshal([]byte(r.FormValue("payload")), &payload)
		is.NoErr(err)
		is.Equal(payload["customField1"], "abc123")

		// check the file
		f, _, err := r.FormFile("chunk")
		is.NoErr(err)
		defer f.Close()
		b, err := ioutil.ReadAll(f)
		is.NoErr(err)
		is.Equal(string(b), "this simulates data")
	}))
	defer processSrv.Close()
	req, err := newRequestFromMediaChunk(client, processSrv.URL, msg)
	is.NoErr(err)
	resp, err := client.Do(req)
	is.NoErr(err)
	is.Equal(resp.StatusCode, http.StatusOK)
	wg.Wait()
}
