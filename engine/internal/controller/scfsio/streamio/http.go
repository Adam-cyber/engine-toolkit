package streamio

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

var DefaultHTTPClient = &http.Client{
	Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: time.Second * 10}).DialContext,
		TLSHandshakeTimeout: time.Second * 10,
	},
}

var initialRetryInterval = time.Second

type httpStreamer struct {
	url        string
	httpClient *http.Client
}

func NewHTTPStreamer(httpClient *http.Client, url string) Streamer {
	return &httpStreamer{url, httpClient}
}

func (h *httpStreamer) Stream(ctx context.Context) *Stream {
	var stream *Stream
	var err error
	interval := initialRetryInterval
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		stream, err = getHTTPStream(ctx, h.httpClient, h.url)
		if err == nil || stream.BytesRead() > 0 {
			break
		}

		if attempt < maxAttempts {
			time.Sleep(interval)
			interval *= 2 // exponential backoff
			Logger.Printf("RETRYING: ATTEMPT %d of %d", attempt+1, maxAttempts)
		}
	}

	stream.SendErr(err)
	return stream
}

func getHTTPStream(ctx context.Context, client *http.Client, url string) (*Stream, error) {
	stream := NewStream()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return stream, fmt.Errorf("unable to generate request for %q: %s", url, err)
	}

	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return stream, err
	}

	stream.OnClose(resp.Body.Close)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return stream, fmt.Errorf("GET %s responded with: %s", url, resp.Status)
	}

	stream = NewStreamFromReader(resp.Body)
	stream.MimeType = resp.Header.Get("Content-Type")
	stream.StartTime = time.Now()

	return stream, err
}
