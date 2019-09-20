package selfdriving_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/veritone/engine-toolkit/engine/internal/selfdriving"
)

func TestRandomSelector(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	inputDir, cleanup := createTestData(t)
	defer cleanup()
	s := &selfdriving.RandomSelector{
		Rand:                    rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:                inputDir,
		InputPattern:            "*.txt",
		Logger:                  log.New(os.Stdout, "", log.LstdFlags),
		PollInterval:            100 * time.Millisecond,
		MinimumModifiedDuration: 100 * time.Millisecond,
	}
	f, err := s.Select(ctx)
	is.NoErr(err)
	defer f.Unlock()
	is.True(f.Path != "")
}

func TestMultiplePatternsSelector(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	inputDir, cleanup := createTestData(t)
	defer cleanup()
	s := &selfdriving.RandomSelector{
		Rand:                    rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:                inputDir,
		InputPattern:            "0.txt|1.txt",
		Logger:                  log.New(os.Stdout, "", log.LstdFlags),
		PollInterval:            100 * time.Millisecond,
		MinimumModifiedDuration: 100 * time.Millisecond,
	}
	f, err := s.Select(ctx)
	is.NoErr(err)
	defer f.Unlock()
	is.True(strings.HasSuffix(f.Path, "0.txt") || strings.HasSuffix(f.Path, "1.txt"))

	f2, err := s.Select(ctx)
	is.NoErr(err)
	defer f2.Unlock()
	is.True(strings.HasSuffix(f.Path, "0.txt") || strings.HasSuffix(f.Path, "1.txt"))
}

func createTestData(t *testing.T) (string, func()) {
	t.Helper()
	is := is.New(t)
	path := filepath.Join("testdata", time.Now().Format(time.RFC3339Nano))
	f := func() {
		is := is.New(t)
		err := os.RemoveAll(path)
		is.NoErr(err)
	}
	inputDir := filepath.Join(path, "input")
	err := os.MkdirAll(inputDir, 0777)
	is.NoErr(err)
	for i := 0; i < 10; i++ {
		txt := fmt.Sprintf("%d.txt", i)
		err := ioutil.WriteFile(filepath.Join(inputDir, txt), []byte(txt), 0777)
		is.NoErr(err)
	}
	return inputDir, f
}
