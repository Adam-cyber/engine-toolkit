package main

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matryer/is"
	"github.com/veritone/engine-toolkit/engine/internal/vericlient"
)

func TestCreate(t *testing.T) {
	is := is.New(t)

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++

		query := r.FormValue("query")
		is.True(strings.Contains(query, `CreateAsset`))

		var vars map[string]interface{}
		err := json.NewDecoder(strings.NewReader(r.FormValue("variables"))).Decode(&vars)
		is.NoErr(err)

		is.Equal(vars["containerId"], "123")
		is.Equal(vars["assetType"], "media")

		file, _, err := r.FormFile("file")
		is.NoErr(err)
		defer file.Close()
		b, err := ioutil.ReadAll(file)
		is.NoErr(err)
		is.Equal(string(b), `{"hello":"world"}`+"\n")

		_, err = io.WriteString(w, `{
			"data": {
				"createAsset": {
					"id": "asset1"
				}
			}
		}`)
		is.NoErr(err)

	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := vericlient.NewClient(http.DefaultClient, "", srv.URL)

	//var buf bytes.Buffer
	createAsset := &AssetCreate{
		ContainerTDOID: "123",
		Name:           "name",
		Description:    "description",
		ContentType:    "application/json",
		AssetType:      "media",
		Details: map[string]interface{}{
			"source": "io.machinebox.facebox.detect",
		},
		FileData: &FileData{
			MD5Sum: "abc",
			Size:   8,
		},
		Body: strings.NewReader(`{"hello":"world"}` + "\n"),
	}

	asset, err := createAsset.Do(ctx, client)
	is.NoErr(err)
	is.Equal(asset.ID, "asset1")

}
