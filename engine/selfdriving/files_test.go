package selfdriving_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/veritone/engine-toolkit/engine/selfdriving"
)

func TestLock(t *testing.T) {
	is := is.New(t)
	txt := "lockme.txt"
	path := filepath.Join("testdata", txt)
	is.NoErr(os.MkdirAll("testdata", 0777))
	err := ioutil.WriteFile(path, []byte(txt), 0777)
	is.NoErr(err)
	f := selfdriving.File{Path: path}

	// lock
	err = f.Lock()
	is.NoErr(err)

	// lock again (should fail)
	err = f.Lock()
	is.Equal(err, selfdriving.ErrFileLocked)

	// unlock
	err = f.Unlock()
	is.NoErr(err)

	// lock again (should succeed)
	err = f.Lock()
	is.NoErr(err)

	// finally, unlock
	err = f.Unlock()
	is.NoErr(err)
}

func Test(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	inputDir, _, cleanup := createTestData(t)
	defer cleanup()
	s := selfdriving.FileSelector{
		Rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:     inputDir,
		InputPattern: "*.txt",
		Logger:       log.New(os.Stdout, "", log.LstdFlags),
	}
	f, err := s.RandomFile(ctx)
	is.NoErr(err)
	//defer f.Unlock()
	log.Println("random file:", f)
	t.Fail()
}

func createTestData(t *testing.T) (string, string, func()) {
	t.Helper()
	is := is.New(t)
	path := filepath.Join("testdata", time.Now().Format(time.RFC3339Nano))
	f := func() {
		is := is.New(t)
		err := os.RemoveAll(path)
		is.NoErr(err)
	}
	inputDir := filepath.Join(path, "input")
	outputDir := filepath.Join(path, "output")
	err := os.MkdirAll(inputDir, 0777)
	is.NoErr(err)
	err = os.MkdirAll(outputDir, 0777)
	is.NoErr(err)
	for i := 0; i < 10; i++ {
		txt := fmt.Sprintf("%d.txt", i)
		err := ioutil.WriteFile(filepath.Join(inputDir, txt), []byte(txt), 0777)
		is.NoErr(err)
	}
	return inputDir, outputDir, f
}
