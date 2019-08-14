package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/matryer/is"
)

func Test(t *testing.T) {

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, engineOutput)
	}))
	defer srv.Close()
	targetURL = srv.URL

	var buf bytes.Buffer
	m := multipart.NewWriter(&buf)
	m.WriteField("startOffsetMS", "1000")
	m.WriteField("endOffsetMS", "2000")
	m.WriteField("width", "640")
	m.WriteField("height", "480")
	f, err := m.CreateFormFile("chunk", "chunk.data")
	if err != nil {
		t.Fatalf("%s", err)
	}
	src, err := os.Open("testdata/monkey.jpg")
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

	err = handleProcess(w, r)
	if err != nil {
		t.Fatalf("%s", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("got status: %d", w.Code)
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
		t.Logf("Expected: %+v", expectedObj)
		t.Logf("  Actual: %+v", obj)
		t.Fatal("incorrect output")
	}

}

func TestTagsToSeries(t *testing.T) {
	is := is.New(t)

	checkResponse := tagboxResponse{}
	info := seriesInfo{
		width:         200,
		height:        200,
		startOffsetMS: 1000,
		endOffsetMS:   2000,
		cacheURI:      "https://machinebox.io/image.jpg",
		MinConfidence: 0.3,
	}
	series := tagsToSeries(checkResponse, info)
	is.Equal(len(series), 0)

	checkResponse = tagboxResponse{
		Tags: []tag{
			{Tag: "monkey", Confidence: 0.6},
			{Tag: "dog", Confidence: 0.4},
			{Tag: "david", Confidence: 0.1},
		},
	}
	series = tagsToSeries(checkResponse, info)
	is.Equal(len(series), 2)
	is.Equal(series[0].Start, 1000)
	is.Equal(series[0].End, 2000)
	is.Equal(series[0].Found, "monkey")
	is.Equal(series[0].Object.Confidence, 0.6)
	is.Equal(series[0].Object.Type, "object")
	is.Equal(series[0].Object.Label, "monkey")
	is.Equal(len(series[0].Object.BoundingPoly), 4)
	is.Equal(series[0].Object.URI, "https://machinebox.io/image.jpg")

	is.Equal(series[1].Object.Confidence, 0.4)
	is.Equal(series[1].Found, "dog")
	is.Equal(series[1].Object.Label, "dog")

}

func round(f float64) float64 {
	return math.Round(f*1000) / 1000
}

const engineOutput = `{
	"success": true,
	"tags": [
	  {
		"tag": "Wildlife",
		"confidence": 0.99
	  },
	  {
		"tag": "Animal",
		"confidence": 0.98
	  },
	  {
		"tag": "Pre-dreadnought battleship",
		"confidence": 0.97
	  },
	  {
		"tag": "Primate",
		"confidence": 0.96
	  },
	  {
		"tag": "Mammal",
		"confidence": 0.95
	  },
	  {
		"tag": "Jungle",
		"confidence": 0.94
	  },
	  {
		"tag": "Monkey",
		"confidence": 0.93
	  },
	  {
		"tag": "Rainforest",
		"confidence": 0.92
	  },
	  {
		"tag": "Ape",
		"confidence": 0.91
	  },
	  {
		"tag": "Zoo",
		"confidence": 0.90
	  }
	],
	"custom_tags": [
	  {
		"tag": "other",
		"confidence": 0.9,
		"id": "72rr9o"
	  }
	]
  }`

const expectedOutput = `{
	"series": [{
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Wildlife",
		"object": {
			"confidence": 0.99,
			"label": "Wildlife",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Animal",
		"object": {
			"confidence": 0.98,
			"label": "Animal",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Pre-dreadnought battleship",
		"object": {
			"confidence": 0.97,
			"label": "Pre-dreadnought battleship",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Primate",
		"object": {
			"confidence": 0.96,
			"label": "Primate",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Mammal",
		"object": {
			"confidence": 0.95,
			"label": "Mammal",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Jungle",
		"object": {
			"confidence": 0.94,
			"label": "Jungle",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Monkey",
		"object": {
			"confidence": 0.93,
			"label": "Monkey",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Rainforest",
		"object": {
			"confidence": 0.92,
			"label": "Rainforest",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Ape",
		"object": {
			"confidence": 0.91,
			"label": "Ape",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}, {
		"startTimeMs": 1000,
		"stopTimeMs": 2000,
		"found": "Zoo",
		"object": {
			"confidence": 0.90,
			"label": "Zoo",
			"type": "object",
			"boundingPoly": [{
				"x": 0,
				"y": 0
			}, {
				"x": 1,
				"y": 0
			}, {
				"x": 1,
				"y": 1
			}, {
				"x": 0,
				"y": 1
			}]
		}
	}]
}`
