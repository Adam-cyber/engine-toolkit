package selfdriving

import (
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	fileSuffixProcessing = ".processing"
	fileSuffixReady      = ".ready"
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

func (f *File) Unlock() {
	lockfile := f.Path + fileSuffixProcessing
	os.Remove(lockfile)
}

func (f *File) Move(moveToDir string) error {
	// remove .ready file (if this fails, it's ok)
	_ = os.Remove(f.Path + fileSuffixReady)

	// move input file (copy & delete is the safest way in containers)
	dest := filepath.Join(moveToDir, filepath.Base(f.Path))
	err := func() error {
		srcFile, err := os.Open(f.Path)
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
	if err := os.Remove(f.Path); err != nil {
		return errors.Wrap(err, "remove source file")
	}
	// create new .ready file
	readyFile := dest + fileSuffixReady
	if err := os.Symlink(dest, readyFile); err != nil {
		return err
	}
	// update the path of the file
	f.Path = dest
	return nil
}
