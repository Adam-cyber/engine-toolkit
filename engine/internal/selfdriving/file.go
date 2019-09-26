package selfdriving

import (
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

// Move moves the source file to the destiation and writes a checksum
// file and an optional ready file.
func (f *File) Move(moveToDir string) error {
	// remove .ready file (if this fails, it's ok)
	_ = os.Remove(f.Path + fileSuffixReady)
	err := os.MkdirAll(moveToDir, 0777)
	if err != nil {
		return err
	}
	// move input file (copy & delete is the safest way in containers)
	dest := filepath.Join(moveToDir, filepath.Base(f.Path))
	// calculate source checksum
	checksum, err := f.calculateCRC32()
	if err != nil {
		return err
	}
	checksumFile := dest + ".sfv"
	if err := ioutil.WriteFile(checksumFile, []byte(filepath.Base(f.Path)+" "+checksum), 0777); err != nil {
		return errors.Wrap(err, "write checksum file")
	}
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

func (f *File) calculateCRC32() (string, error) {
	src, err := os.Open(f.Path)
	if err != nil {
		return "", errors.Wrap(err, "open source file")
	}
	defer src.Close()
	checksum := crc32.NewIEEE()
	if _, err := io.Copy(checksum, src); err != nil {
		return "", errors.Wrap(err, "read source file")
	}
	checksumStr := fmt.Sprintf("%x", checksum.Sum(nil))
	return checksumStr, nil
}

// FormatOutputPattern injects time components into the pattern string.
// The following strings will be replaced:
//  yyyy - year
//  mm - month
//  dd - day
//  hh - hour
//  mm - minute
//  ss - seconds
// For example, "year-yyy/mm/dd" creates a folder called "year-2019"
// inside which would be a folder for the current month, and inside that
// another folder for the day. The output files will appear inside that
// folder.
func FormatOutputPattern(now time.Time, pattern string) string {
	s := pattern
	s = strings.Replace(s, "yyyy", strconv.Itoa(now.Year()), -1)
	s = strings.Replace(s, "mm", fmt.Sprintf("%02d", now.Month()), -1)
	s = strings.Replace(s, "dd", fmt.Sprintf("%02d", now.Day()), -1)
	s = strings.Replace(s, "hh", fmt.Sprintf("%02d", now.Hour()), -1)
	s = strings.Replace(s, "mm", fmt.Sprintf("%02d", now.Minute()), -1)
	s = strings.Replace(s, "ss", fmt.Sprintf("%02d", now.Second()), -1)
	return s
}
