package selfdriving

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

type Processor struct {
	Logger    *log.Logger
	Files     FileSelector
	Process   func(File) error
	OutputDir string
}

func (p *Processor) Run(ctx context.Context) error {
	if err := os.MkdirAll(p.OutputDir, 0777); err != nil {
		p.Logger.Println("failed to make output directory:", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := func() error {
			file, err := p.Files.Select(ctx)
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
	time.Sleep(250 * time.Millisecond)
	return p.Process(file)
}

func (p *Processor) moveFile(file File) error {
	// remove .done file
	if err := os.Remove(file.Path + fileSuffixReady); err != nil {
		p.Logger.Printf("failed to remove %s file: %s\n", fileSuffixReady, err)
	}
	// move input file
	dest := filepath.Join(p.OutputDir, filepath.Base(file.Path))
	if err := os.Rename(file.Path, dest); err != nil {
		return errors.Wrap(err, "rename")
	}
	// create new .done file
	doneFile := dest + fileSuffixReady
	if err := os.Symlink(dest, doneFile); err != nil {
		return err
	}
	return nil
}
