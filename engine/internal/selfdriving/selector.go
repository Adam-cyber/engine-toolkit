package selfdriving

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileSelector interface {
	Select(ctx context.Context) (File, error)
}

type FileSelectorFunc func(context.Context) (File, error)

func (fn FileSelectorFunc) Select(ctx context.Context) (File, error) {
	return fn(ctx)
}

type RandomSelector struct {
	Rand *rand.Rand
	// PollInterval is how long to wait after no-suitable files were
	// found.
	PollInterval time.Duration
	// MinimumModifiedDuration is the amount of time to wait after
	// a file is modified before considering it for selection.
	MinimumModifiedDuration time.Duration
	// Logger is where log output will be sent.
	Logger *log.Logger
	// InputDir is the input directory.
	InputDir string
	// InputPattern is the glob pattern for input files.
	// If empty, all files will be matched.
	InputPattern string
	// WaitForReadyFiles will only select files that have a .done
	// file alongside it. This should be used when the output from another
	// engine is the input to this engine.
	WaitForReadyFiles bool
}

// Select will return a locked file. Unlock must be called on the file.
// Select will block until a file is available.
func (s *RandomSelector) Select(ctx context.Context) (File, error) {
	if s.PollInterval == 0 {
		s.PollInterval = 1 * time.Minute
	}
	if s.MinimumModifiedDuration == 0 {
		s.MinimumModifiedDuration = 1 * time.Minute
	}
	for {
		if err := ctx.Err(); err != nil {
			return File{}, err
		}
		fileCandidates := make([]string, 0)
		err := filepath.Walk(s.InputDir, func(path string, info os.FileInfo, err error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, fileSuffixProcessing) {
				// ignore processing files
				return nil
			}
			if strings.HasSuffix(path, fileSuffixError) {
				// ignore error files
				return nil
			}
			if s.WaitForReadyFiles && strings.HasSuffix(path, fileSuffixReady) {
				// trim off .done
				path = path[:len(path)-len(fileSuffixReady)]
				match, err := s.matchesInputPattern(path)
				if err != nil {
					return err
				}
				if match {
					fileCandidates = append(fileCandidates, path)
				}
			} else if !s.WaitForReadyFiles {
				if time.Now().Sub(info.ModTime()) < s.MinimumModifiedDuration {
					return nil
				}
				match, err := s.matchesInputPattern(path)
				if err != nil {
					return err
				}
				if match {
					fileCandidates = append(fileCandidates, path)
				}
			}
			return nil
		})
		if err != nil {
			s.Logger.Println("ERROR:", err)
		}
		if len(fileCandidates) == 0 {
			s.Logger.Println("no files, snoozing for", s.PollInterval)
			time.Sleep(s.PollInterval)
			continue
		}
		// select random file
		file := File{
			Path: fileCandidates[s.Rand.Intn(len(fileCandidates))],
		}
		if err := file.Lock(); err != nil {
			continue
		}
		return file, nil
	}
}

func (s *RandomSelector) matchesInputPattern(path string) (bool, error) {
	if s.InputPattern == "" {
		return true, nil
	}
	return filepath.Match(s.InputPattern, filepath.Base(path))
}
