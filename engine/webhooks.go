package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	"github.com/pkg/errors"
	"github.com/veritone/engine-toolkit/engine/internal/selfdriving"
)

func (e *Engine) newRequestFromFile(processURL string, file selfdriving.File) (*http.Request, error) {

	pathhash := hash(file.Path)
	mimeType := mime.TypeByExtension(filepath.Ext(file.Path))
	var width, height int
	if strings.HasPrefix(mimeType, "image") {
		width, height = e.getImageWidthAndHeight(file.Path)
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.WriteField("chunkMimeType", mimeType); err != nil {
		return nil, errors.Wrap(err, "cannot write to multipart writer")
	}
	_ = w.WriteField("chunkUUID", "fs-"+pathhash)
	_ = w.WriteField("chunkIndex", "0")
	_ = w.WriteField("startOffsetMS", "0")
	_ = w.WriteField("endOffsetMS", "0")
	_ = w.WriteField("width", strconv.Itoa(width))
	_ = w.WriteField("height", strconv.Itoa(height))
	_ = w.WriteField("libraryId", "")            // not supported
	_ = w.WriteField("libraryEngineModelId", "") // not supported
	_ = w.WriteField("cacheURI", "")             // not supported
	_ = w.WriteField("veritoneApiBaseUrl", "")   // not supported
	_ = w.WriteField("token", "")                // not supported
	_ = w.WriteField("payload", "")              // not supported
	chunk, err := w.CreateFormFile("chunk", "chunk.data")
	if err != nil {
		return nil, err
	}
	f, err := os.Open(file.Path)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}
	defer f.Close()
	if _, err := io.Copy(chunk, f); err != nil {
		return nil, errors.Wrap(err, "copy to chunk")
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, processURL, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "veritone-engine-toolkit; self-driving-mode")
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

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

func hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (e *Engine) getImageWidthAndHeight(filename string) (int, int) {
	f, err := os.Open(filename)
	if err != nil {
		e.logDebug("getImageWidthAndHeight: cannot open file:", err)
		return 0, 0
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		e.logDebug("getImageWidthAndHeight: cannot decode image:", err)
		return 0, 0
	}
	b := img.Bounds()
	width := b.Dx()
	height := b.Dy()
	return width, height
}
