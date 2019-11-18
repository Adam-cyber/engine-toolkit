package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestManifest(t *testing.T) {
	is := is.New(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/manifest.json", nil)
	eng := Engine{}
	h := eng.handleManifest("testdata/manifest-ok.json")
	h.ServeHTTP(w, req)
	is.Equal(w.Code, http.StatusOK)

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/manifest.json", nil)
	h = eng.handleManifest("testdata/manifest-bad-engineMode.json")
	h.ServeHTTP(w, req)
	is.Equal(w.Code, http.StatusInternalServerError)
	is.Equal(strings.TrimSpace(w.Body.String()), `manifest.json: engineMode: should be "chunk" not "nope"`)
}
