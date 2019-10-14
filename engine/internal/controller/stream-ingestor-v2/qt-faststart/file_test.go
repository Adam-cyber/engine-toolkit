package qtfaststart

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAndRearrangeQT(t *testing.T) {

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/white.mov"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	qtf, err := Parse(file)
	assert.Nil(t, err)

	assert.False(t, qtf.FastStartEnabled())

	file, err = os.Open(sampleFile)
	if err != nil {
		t.Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	// create new stream
	r, err := NewReader(file, qtf, false)
	assert.Nil(t, err)

	// parse the processed stream and see if it's been re-arranged
	b, err := ioutil.ReadAll(r)
	qtf, err = Parse(bytes.NewReader(b))

	assert.Nil(t, err)
	assert.True(t, qtf.FastStartEnabled())
}
