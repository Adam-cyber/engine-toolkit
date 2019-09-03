package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/rwcarlsen/goexif/exif"
)

func main() {
	if err := http.ListenAndServe("0.0.0.0:8080", newServer()); err != nil {
		fmt.Fprintf(os.Stderr, "exif: %s", err)
		os.Exit(1)
	}
}

func newServer() *http.ServeMux {
	s := http.NewServeMux()
	s.HandleFunc("/readyz", handleReady)
	s.HandleFunc("/process", handleProcess)
	return s
}

// handleProcess runs the incoming chunk through an exif decoder and
// writes the results to the response.
func handleProcess(w http.ResponseWriter, r *http.Request) {
	startOffsetMS, err := strconv.Atoi(r.FormValue("startOffsetMS"))
	if err != nil {
		http.Error(w, "exif: "+err.Error(), http.StatusBadRequest)
		return
	}
	stopTimeMs, err := strconv.Atoi(r.FormValue("endOffsetMS"))
	if err != nil {
		http.Error(w, "exif: "+err.Error(), http.StatusBadRequest)
		return
	}
	f, _, err := r.FormFile("chunk")
	if err != nil {
		http.Error(w, "exif: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer f.Close()
	var resp response
	resp.Series = []item{{
		StartTimeMs: startOffsetMS,
		StopTimeMs:  stopTimeMs,
	}}
	x, err := exif.Decode(f)
	if err != nil {
		resp.Series[0].Object.Vendor.ExifError = err.Error()
	}
	resp.Series[0].Object.Vendor.Exif = x
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
}

type response struct {
	Series []item `json:"series"`
}

type vendor struct {
	Exif       *exif.Exif `json:"exif,omitempty"`
	ExifError  string     `json:"exifError,omitempty"`
	FormValues url.Values `json:"formValues"`
}

type item struct {
	StartTimeMs int `json:"startTimeMs"`
	StopTimeMs  int `json:"stopTimeMs"`
	Object      struct {
		Vendor vendor `json:"vendor"`
	} `json:"object"`
}

// handleReady just returns OK - if it is doing that, then we are ready
// to receive processing jobs.
func handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
