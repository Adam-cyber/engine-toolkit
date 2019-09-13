package selfdriving

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	fileSuffixProcessing = ".processing"
	fileSuffixReady      = ".ready"
	fileSuffixError      = ".error"
)

var ErrFileLocked = errors.New("file already locked")

type File struct {
	Path     string
	lockPath string
}

func (f *File) Lock() error {
	lockfile := f.Path + fileSuffixProcessing
	if err := os.Symlink(f.Path, lockfile); err != nil {
		return ErrFileLocked
	}
	f.lockPath = lockfile
	return nil
}

func (f *File) Unlock() {
	// it's ok if this errors, it means it's already unlocked
	_ = os.Remove(f.lockPath)
}

func (f *File) Ready() error {
	readyFile := f.Path + fileSuffixReady
	return os.Symlink(f.Path, readyFile)
}

func (f *File) NotReady() {
	// remove .ready file (if this fails, it's ok)
	_ = os.Remove(f.Path + fileSuffixReady)
}

func (f *File) Move(moveToDir string) error {
	// remove .ready file (if this fails, it's ok)
	_ = os.Remove(f.Path + fileSuffixReady)

	err := os.MkdirAll(moveToDir, 0777)
	if err != nil {
		return err
	}
	// move input file (copy & delete is the safest way in containers)
	dest := filepath.Join(moveToDir, filepath.Base(f.Path))
	err = func() error {
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

func (f *File) WriteErr(err error) {
	if err == nil {
		return
	}
	errorFile := f.Path + fileSuffixError
	msg := []byte(err.Error())
	_ = ioutil.WriteFile(errorFile, msg, 0777)
	return
}
