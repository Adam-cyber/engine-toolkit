package streamio

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/white.mov"

	fr := NewFFMPEGReader(sampleFile)

	ctx := context.Background()
	sr := fr.Stream(ctx)

	b, err := ioutil.ReadAll(sr)
	assert.Nil(t, err)
	assert.True(t, len(b) > 0)
}
