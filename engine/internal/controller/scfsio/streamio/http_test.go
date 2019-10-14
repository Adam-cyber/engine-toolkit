package streamio

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/stretchr/testify/suite"
)

type httpTestSuite struct {
	suite.Suite
}

func (t *httpTestSuite) SetupTest() {
	initialRetryInterval = time.Millisecond
}

func (t *httpTestSuite) TestStreamFail() {
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	reader := NewHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background())
	if t.NotNil(stream) {
		go io.Copy(ioutil.Discard, stream)
		err := stream.ErrWait()
		if t.Error(err) {
			t.Contains(err.Error(), "503 Service Unavailable")
		}
	}

	t.Equal(5, count)
}

func (t *httpTestSuite) TestStreamSuccess() {
	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/image.png"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusOK)
		headers := w.Header()
		headers["Content-Type"] = []string{"image/png"}
		w.Write(b)
		w.Write(nil)
	}))
	defer srv.Close()

	reader := NewHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background())
	if t.NotNil(stream) {
		go func() {
			n, err := io.Copy(ioutil.Discard, stream)
			t.NoError(err)
			t.True(n > 0)
		}()
		t.NoError(stream.ErrWait())
		t.Equal("", stream.FfmpegFormat.String())
		t.Equal("image/png", stream.MimeType)
		t.InDelta(time.Now().Unix(), stream.StartTime.Unix(), 1)
	}

	t.Equal(1, count)
}

func (t *httpTestSuite) TestStreamRetry1stFailThenSucceeed() {
	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/image.png"
	b, err := ioutil.ReadFile(sampleFile)
	if err != nil {
		t.Failf("failed to read sample file %q: %s", sampleFile, err)
	}

	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write(nil)
		} else {
			w.WriteHeader(http.StatusOK)
			headers := w.Header()
			headers["Content-Type"] = []string{"image/png"}
			w.Write(b)
			w.Write(nil)
		}
		count++
	}))
	defer srv.Close()

	reader := NewHTTPStreamer(http.DefaultClient, srv.URL)
	stream := reader.Stream(context.Background())
	if t.NotNil(stream) {
		go func() {
			n, err := io.Copy(ioutil.Discard, stream)
			t.NoError(err)
			t.True(n > 0)
		}()
		t.NoError(stream.ErrWait())
		t.Equal("", stream.FfmpegFormat.String())
		t.Equal("image/png", stream.MimeType)
		t.InDelta(time.Now().Unix(), stream.StartTime.Unix(), 1)
	}

	t.Equal(2, count)
}
