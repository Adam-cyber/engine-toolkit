package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	// set the target URL here
	targetURL = ""
)

func main() {
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		err := handleProcess(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	if err := http.ListenAndServe("0.0.0.0:8080", nil); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func handleProcess(w http.ResponseWriter, r *http.Request) error {
	chunkR, _, err := r.FormFile("chunk")
	if err != nil {
		return err
	}
	defer chunkR.Close()
	// get other fields like this:  r.FormValue("startOffsetMS")
	var buf bytes.Buffer
	body := multipart.NewWriter(&buf)
	chunkW, err := body.CreateFormFile("file", "")
	if err != nil {
		return err
	}
	if _, err := io.Copy(chunkW, chunkR); err != nil {
		return err
	}
	if err := body.Close(); err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, targetURL, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", body.FormDataContentType())
	req.Header.Set("Accept", "application/json; charset=utf-8")
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("%d", resp.StatusCode)
		return err
	}
	var respBody tagboxResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return err
	}
	startOffsetMS, err := strconv.Atoi(r.FormValue("startOffsetMS"))
	if err != nil {
		return err
	}
	endOffsetMS, err := strconv.Atoi(r.FormValue("endOffsetMS"))
	if err != nil {
		return err
	}
	width, err := strconv.Atoi(r.FormValue("width"))
	if err != nil {
		return err
	}
	height, err := strconv.Atoi(r.FormValue("height"))
	if err != nil {
		return err
	}
	var payload struct {
		MinConfidence float64 `json:"minConfidence"` // custom field
	}
	payloadJSON := r.FormValue("payload")
	if payloadJSON != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return err
		}
	}
	info := seriesInfo{
		width:         width,
		height:        height,
		startOffsetMS: startOffsetMS,
		endOffsetMS:   endOffsetMS,
		cacheURI:      r.FormValue("cacheURI"),
		MinConfidence: payload.MinConfidence,
	}
	var response struct {
		Series []seriesItem `json:"series"`
	}
	response.Series = tagsToSeries(respBody, info)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		return err
	}
	return nil
}

type tagboxResponse struct {
	Tags []tag `json:"tags"`
}

type tag struct {
	Tag        string  `json:"tag"`
	Confidence float64 `json:"confidence"`
}

func tagsToSeries(checkResponse tagboxResponse, info seriesInfo) []seriesItem {
	var items []seriesItem
	for _, tag := range checkResponse.Tags {
		if tag.Confidence < info.MinConfidence {
			continue
		}
		item := seriesItem{
			Found: tag.Tag,
			Start: info.startOffsetMS,
			End:   info.endOffsetMS,
			Object: object{
				Label:        tag.Tag,
				Type:         "object",
				URI:          info.cacheURI,
				Confidence:   tag.Confidence,
				BoundingPoly: wholeImage,
			},
		}
		items = append(items, item)
	}
	return items
}

type seriesItem struct {
	Start              int    `json:"startTimeMs"`
	End                int    `json:"stopTimeMs"`
	LibraryID          string `json:"libraryId,omitempty"`
	EntityID           string `json:"entityId,omitempty"`
	EntityIdentifierID string `json:"entityIdentifierId,omitempty"`
	Found              string `json:"found"`
	Object             object `json:"object"`
}

type object struct {
	Confidence   float64 `json:"confidence"`
	Label        string  `json:"label,omitempty"`
	Type         string  `json:"type,omitempty"`
	LibraryID    string  `json:"libraryId,omitempty"`
	EntityID     string  `json:"entityId,omitempty"`
	URI          string  `json:"uri,omitempty"`
	BoundingPoly []point `json:"boundingPoly,omitempty"`
}

type seriesInfo struct {
	width         int
	height        int
	startOffsetMS int
	endOffsetMS   int
	cacheURI      string
	MinConfidence float64
}

type point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

var wholeImage = []point{
	{X: 0, Y: 0},
	{X: 1, Y: 0},
	{X: 1, Y: 1},
	{X: 0, Y: 1},
}
