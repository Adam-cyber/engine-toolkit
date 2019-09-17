package selfdriving_test

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/veritone/engine-toolkit/engine/internal/selfdriving"
)

func TestProcessing(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	inputDir, cleanup := createTestData(t)
	defer cleanup()
	processFunc := func(outputFile string, f selfdriving.File) error {
		log.Println("testing: process", f.Path, outputFile)
		time.Sleep(1 * time.Second)
		return nil
	}
	s := &selfdriving.RandomSelector{
		Rand:                    rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:                inputDir,
		InputPattern:            "*.txt",
		Logger:                  log.New(os.Stdout, "", log.LstdFlags),
		PollInterval:            100 * time.Millisecond,
		MinimumModifiedDuration: 100 * time.Millisecond,
	}
	outputDir := filepath.Join(filepath.Dir(inputDir), "output")
	errorsDir := filepath.Join(filepath.Dir(inputDir), "errors")
	p := &selfdriving.Processor{
		Selector:   s,
		Logger:     log.New(os.Stdout, "", log.LstdFlags),
		MoveToDir:  outputDir,
		ResultsDir: outputDir,
		ErrDir:     errorsDir,
		Process:    processFunc,
	}
	if err := p.Run(ctx); err != nil {
		t.Logf("run: %v", err)
	}
}

func TestProcessingPipeline(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	inputDir, cleanup := createTestData(t)
	defer cleanup()

	dir := filepath.Dir(inputDir)
	output1Dir := filepath.Join(dir, "2")
	output2Dir := filepath.Join(dir, "3")
	output3Dir := filepath.Join(dir, "output")

	errorsDir := filepath.Join(filepath.Dir(dir), "errors")
	is.NoErr(os.MkdirAll(output1Dir, 0777))
	is.NoErr(os.MkdirAll(output2Dir, 0777))
	is.NoErr(os.MkdirAll(output3Dir, 0777))

	s1 := &selfdriving.RandomSelector{
		PollInterval:      100 * time.Millisecond,
		Rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:          inputDir,
		InputPattern:      "*.txt",
		Logger:            log.New(os.Stdout, "", log.LstdFlags),
		WaitForReadyFiles: false,
	}
	s2 := &selfdriving.RandomSelector{
		PollInterval:      100 * time.Millisecond,
		Rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:          output1Dir,
		InputPattern:      "*.txt",
		Logger:            log.New(os.Stdout, "", log.LstdFlags),
		WaitForReadyFiles: true,
	}
	s3 := &selfdriving.RandomSelector{
		PollInterval:      100 * time.Millisecond,
		Rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
		InputDir:          output2Dir,
		InputPattern:      "*.txt",
		Logger:            log.New(os.Stdout, "", log.LstdFlags),
		WaitForReadyFiles: true,
	}

	p1 := &selfdriving.Processor{
		Selector:   s1,
		Logger:     log.New(os.Stdout, "", log.LstdFlags),
		MoveToDir:  output1Dir,
		ResultsDir: output1Dir,
		ErrDir:     errorsDir,
		Process: func(outputFile string, f selfdriving.File) error {
			log.Println("testing: process 1:", f.Path, "to", outputFile)
			time.Sleep(250 * time.Millisecond)
			return nil
		},
	}
	p2 := &selfdriving.Processor{
		Selector:   s2,
		Logger:     log.New(os.Stdout, "", log.LstdFlags),
		MoveToDir:  output2Dir,
		ResultsDir: output2Dir,
		ErrDir:     errorsDir,
		Process: func(outputFile string, f selfdriving.File) error {
			log.Println("testing: process 2:", f.Path, "to", outputFile)
			time.Sleep(250 * time.Millisecond)
			return nil
		},
	}
	p3 := &selfdriving.Processor{
		Selector:   s3,
		Logger:     log.New(os.Stdout, "", log.LstdFlags),
		MoveToDir:  output3Dir,
		ResultsDir: output3Dir,
		ErrDir:     errorsDir,
		Process: func(outputFile string, f selfdriving.File) error {
			log.Println("testing: process 3:", f.Path, "to", outputFile)
			time.Sleep(250 * time.Millisecond)
			return nil
		},
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p1.Run(ctx); err != nil {
			t.Logf("run: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p2.Run(ctx); err != nil {
			t.Logf("run: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p3.Run(ctx); err != nil {
			t.Logf("run: %v", err)
		}
	}()

	wg.Wait()

}
