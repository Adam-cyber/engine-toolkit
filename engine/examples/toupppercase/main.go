package main

import (
	"bufio"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
)

func main() {
	if err := http.ListenAndServe("0.0.0.0:8080", newServer()); err != nil {
		fmt.Fprintf(os.Stderr, "touppercase: %s", err)
		os.Exit(1)
	}
}

func newServer() *http.ServeMux {
	s := http.NewServeMux()
	s.HandleFunc("/readyz", handleReady)
	s.HandleFunc("/process", handleProcess)
	return s
}

// handleProcess runs the incoming chunk through an touppercase decoder and
// writes the results to the response.
func handleProcess(w http.ResponseWriter, r *http.Request) {
	f, _, err := r.FormFile("chunk")
	if err != nil {
		http.Error(w, "touppercase: "+err.Error(), http.StatusBadRequest)
		return
	}
	chunkUUID := r.FormValue("chunkUUID")

	defer f.Close()
	mw := multipart.NewWriter(w)
	defer mw.Close()

	w.Header().Set("Content-Type", mw.FormDataContentType())
	out, err := mw.CreateFormFile("assets", chunkUUID+"-uppercase.txt")
	if err != nil {
		http.Error(w, "touppercase: "+err.Error(), http.StatusBadRequest)
		return
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		upper := strings.ToUpper(s.Text())
		io.WriteString(out, upper)
		io.WriteString(out, "\n")
	}
	err = s.Err()
	if err != nil {
		http.Error(w, "touppercase: "+err.Error(), http.StatusBadRequest)
		return
	}
}

// handleReady just returns OK - if it is doing that, then we are ready
// to receive processing jobs.
func handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
