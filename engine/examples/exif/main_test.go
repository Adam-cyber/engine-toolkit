package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
)

func TestProcess(t *testing.T) {
	var buf bytes.Buffer
	m := multipart.NewWriter(&buf)
	m.WriteField("startOffsetMS", "1000")
	m.WriteField("endOffsetMS", "2000")
	f, err := m.CreateFormFile("chunk", "chunk.data")
	if err != nil {
		t.Fatalf("%s", err)
	}
	src, err := os.Open("testdata/animal.jpg")
	if err != nil {
		t.Fatalf("%s", err)
	}
	if _, err := io.Copy(f, src); err != nil {
		t.Fatalf("%s", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("%s", err)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/process", &buf)
	r.Header.Set("Content-Type", m.FormDataContentType())
	srv := newServer()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("got status: %d: %s", w.Code, w.Body.String())
	}
	var obj interface{}
	if err := json.NewDecoder(w.Body).Decode(&obj); err != nil {
		t.Fatalf("%s", err)
	}
	var expectedObj interface{}
	if err := json.Unmarshal([]byte(expectedOutput), &expectedObj); err != nil {
		t.Fatalf("%s", err)
	}
	if !reflect.DeepEqual(obj, expectedObj) {
		t.Fatal("incorrect output")
	}
}

const expectedOutput = `{
	"series": [{
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"object": {
			"vendor": {
				"exif": {
					"ApertureValue": ["368640/65536"],
					"ColorSpace": [1],
					"ComponentsConfiguration": "",
					"CustomRendered": [0],
					"DateTime": "2008:07:31 10:38:11",
					"DateTimeDigitized": "2008:05:30 15:56:01",
					"DateTimeOriginal": "2008:05:30 15:56:01",
					"ExifIFDPointer": [214],
					"ExifVersion": "0221",
					"ExposureBiasValue": ["0/1"],
					"ExposureMode": [1],
					"ExposureProgram": [1],
					"ExposureTime": ["1/160"],
					"FNumber": ["71/10"],
					"Flash": [9],
					"FlashpixVersion": "0100",
					"FocalLength": ["135/1"],
					"FocalPlaneResolutionUnit": [2],
					"FocalPlaneXResolution": ["3888000/876"],
					"FocalPlaneYResolution": ["2592000/583"],
					"GPSInfoIFDPointer": [978],
					"GPSVersionID": [2, 2, 0, 0],
					"ISOSpeedRatings": [100],
					"InteroperabilityIFDPointer": [948],
					"InteroperabilityIndex": "R98",
					"Make": "Canon",
					"MeteringMode": [5],
					"Model": "Canon EOS 40D",
					"Orientation": [1],
					"PixelXDimension": [100],
					"PixelYDimension": [68],
					"ResolutionUnit": [2],
					"SceneCaptureType": [0],
					"ShutterSpeedValue": ["483328/65536"],
					"Software": "GIMP 2.4.5",
					"SubSecTime": "00",
					"SubSecTimeDigitized": "00",
					"SubSecTimeOriginal": "00",
					"ThumbJPEGInterchangeFormat": [1090],
					"ThumbJPEGInterchangeFormatLength": [1378],
					"UserComment": "",
					"WhiteBalance": [0],
					"XResolution": ["72/1"],
					"YCbCrPositioning": [2],
					"YResolution": ["72/1"]
				}
			}
		}
	}]
}`
