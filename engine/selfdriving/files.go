package selfdriving

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var ErrFileLocked = errors.New("file already locked")

const (
	fileSuffixProcessing = ".processing"
)

type File struct {
	Path string
}

func (f *File) Lock() error {
	lockfile := f.Path + fileSuffixProcessing
	if err := os.Symlink(f.Path, lockfile); err != nil {
		return ErrFileLocked
	}
	return nil
}

func (f *File) Unlock() error {
	lockfile := f.Path + fileSuffixProcessing
	if err := os.Remove(lockfile); err != nil {
		return err
	}
	return nil
}

type FileSelector struct {
	Rand *rand.Rand

	PollInterval time.Duration
	Logger       *log.Logger
	InputDir     string
	// InputPattern is the glob pattern for input files.
	// If empty, all files will be matched.
	InputPattern string
}

// RandomFile will return a locked file. Unlock must be called.
func (s *FileSelector) RandomFile(ctx context.Context) (File, error) {
	for {
		if err := ctx.Err(); err != nil {
			return File{}, err
		}
		filesList := make([]string, 0)
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
			if s.InputPattern != "" {
				match, err := filepath.Match(s.InputPattern, filepath.Base(path))
				if err != nil {
					return err
				}
				if !match {
					s.Logger.Println(s.InputPattern, "skipping file", path)
					return nil
				}
			}
			filesList = append(filesList, path)
			return nil
		})
		if err != nil {
			log.Println("ERROR:", err)
		}
		if len(filesList) == 0 {
			time.Sleep(s.PollInterval)
			continue
		}
		randomFile := File{
			Path: filesList[s.Rand.Intn(len(filesList))],
		}
		if err := randomFile.Lock(); err != nil {
			continue
		}
		return randomFile, nil
	}
}

func (s *FileSelector) RandomFiles(ctx context.Context) chan File {
	filesChan := make(chan File)
	go func() {
		defer close(filesChan)
		for {
			if err := ctx.Err(); err != nil {
				return
			}

		}
	}()
	return filesChan
}
