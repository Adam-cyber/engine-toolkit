package streamio

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileStreamWriter(t *testing.T) {
	ctx := context.Background()

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/image.png"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.Fatalf("failed to read sample file %q: %s", sampleFile, err)
	}

	fStat, err := file.Stat()
	assert.Nil(t, err)

	sr := NewFileReader(file, "image/png", "")
	stream := sr.Stream(ctx)

	tempFile, err := ioutil.TempFile(".", "unittest")
	if err != nil {
		t.Fatalf("failed to createw temp file: %s", err)
	}
	defer os.Remove(tempFile.Name())

	sw := NewFileStreamWriter(tempFile)
	err = sw.WriteStream(ctx, stream)
	assert.Nil(t, err)

	assert.Equal(t, fStat.Size(), sw.BytesWritten())
}
