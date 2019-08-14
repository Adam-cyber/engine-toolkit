package main

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"

	"github.com/pkg/errors"
)

func newRequestFromMediaChunk(client *http.Client, processURL string, msg mediaChunkMessage) (*http.Request, error) {
	payload, err := msg.unmarshalPayload()
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal payload")
	}
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	// check the first error, and if it's ok - ignore the rest
	if err = w.WriteField("chunkMimeType", msg.MIMEType); err != nil {
		return nil, errors.Wrap(err, "cannot write to multipart writer")
	}
	_ = w.WriteField("chunkUUID", msg.ChunkUUID)
	_ = w.WriteField("chunkIndex", strconv.Itoa(msg.ChunkIndex))
	_ = w.WriteField("startOffsetMS", strconv.Itoa(msg.StartOffsetMS))
	_ = w.WriteField("endOffsetMS", strconv.Itoa(msg.EndOffsetMS))
	_ = w.WriteField("width", strconv.Itoa(msg.Width))
	_ = w.WriteField("height", strconv.Itoa(msg.Height))
	_ = w.WriteField("libraryId", payload.LibraryID)
	_ = w.WriteField("libraryEngineModelId", payload.LibraryEngineModelID)
	_ = w.WriteField("cacheURI", msg.CacheURI)
	_ = w.WriteField("veritoneApiBaseUrl", payload.VeritoneAPIBaseURL)
	_ = w.WriteField("token", payload.Token)
	_ = w.WriteField("payload", string(msg.TaskPayload))
	if msg.CacheURI != "" {
		f, err := w.CreateFormFile("chunk", "chunk.data")
		if err != nil {
			return nil, err
		}
		resp, err := client.Get(msg.CacheURI)
		if err != nil {
			return nil, errors.Wrap(err, "download chunk file from source")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, errors.Wrapf(err, "download chunk file from source: status code %v", resp.Status)
		}
		if _, err := io.Copy(f, resp.Body); err != nil {
			return nil, errors.Wrap(err, "read chunk file from source")
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, processURL, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "veritone-engine-toolkit")
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}
