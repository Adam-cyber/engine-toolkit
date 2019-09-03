package selfdriving

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

type Processor struct {
	Logger     *log.Logger
	Selector   FileSelector
	Process    func(outputFile string, file File) error
	MoveToDir  string
	ResultsDir string
}

func (p *Processor) Run(ctx context.Context) error {
	if err := os.MkdirAll(p.MoveToDir, 0777); err != nil {
		p.Logger.Println("failed to make output directory:", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := func() error {
			file, err := p.Selector.Select(ctx)
			if err != nil {
				return err
			}
			defer file.Unlock()
			if err := p.processFile(file); err != nil {
				p.Logger.Println("ERROR: process:", err)
			}
			if err := p.moveFile(file); err != nil {
				p.Logger.Println("ERROR: move file:", err)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
}

func (p *Processor) processFile(file File) error {
	now := time.Now()
	dir := filepath.Join(p.ResultsDir, now.Format("2006-01"), now.Format("20060102-1500"))
	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}
	outputFile := filepath.Join(dir, filepath.Base(file.Path)+".json")
	return p.Process(outputFile, file)
}

func (p *Processor) moveFile(file File) error {
	// remove .ready file (if this fails, it's ok)
	_ = os.Remove(file.Path + fileSuffixReady)
	// move input file
	dest := filepath.Join(p.MoveToDir, filepath.Base(file.Path))
	err := func() error {
		srcFile, err := os.Open(file.Path)
		if err != nil {
			return errors.Wrap(err, "open source file")
		}
		defer srcFile.Close()
		destFile, err := os.Create(dest)
		if err != nil {
			return errors.Wrap(err, "create output file")
		}
		defer destFile.Close()
		if _, err := io.Copy(destFile, srcFile); err != nil {
			return errors.Wrap(err, "copy")
		}
		return nil
	}()
	if err != nil {
		return err
	}
	if err := os.Remove(file.Path); err != nil {
		return errors.Wrap(err, "remove source file")
	}
	// create new .ready file
	readyFile := dest + fileSuffixReady
	if err := os.Symlink(dest, readyFile); err != nil {
		return err
	}
	return nil
}
