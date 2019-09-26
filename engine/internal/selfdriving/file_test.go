package selfdriving_test

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/pkg/errors"
	"github.com/veritone/engine-toolkit/engine/internal/selfdriving"
)

func TestFormatOutputPattern(t *testing.T) {
	is := is.New(t)
	log.Println(time.Now().Format(time.RFC3339))
	now, err := time.Parse(time.RFC3339, "2019-09-17T16:06:36+00:00")
	is.NoErr(err)
	tests := map[string]string{
		"":                    "",
		"static/folders":      "static/folders",
		"yyyy/mm/dd":          "2019/09/17",
		"year-yyyy/mm/dd":     "year-2019/09/17",
		"yyyy mm dd hh:mm:ss": "2019 09 17 16:09:36",
		"hh_mm_ss":            "16_09_36",
	}
	for in, expected := range tests {
		actual := selfdriving.FormatOutputPattern(now, in)
		is.Equal(actual, expected)
	}
}

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
	f.Unlock()

	// lock again (should succeed)
	err = f.Lock()
	is.NoErr(err)

	// finally, unlock
	f.Unlock()
}

func TestMove(t *testing.T) {
	is := is.New(t)

	path := filepath.Join("testdata", "moveme.txt")
	is.NoErr(os.MkdirAll("testdata", 0777))

	err := ioutil.WriteFile(path, []byte("data for testing"), 0777)
	is.NoErr(err)
	defer func() {
		os.Remove(path)
		os.RemoveAll(filepath.Join("testdata", "completed"))
	}()

	f := selfdriving.File{Path: path}
	// move
	dst := filepath.Join("testdata", "completed")
	err = f.Move(dst)
	is.NoErr(err)
	is.Equal(f.Path, filepath.Join(dst, "moveme.txt"))

	// check for ready file
	_, err = os.Lstat(filepath.Join("testdata", "completed", "moveme.txt.ready"))
	is.NoErr(err) // ready file

	// check for crc32
	b, err := ioutil.ReadFile(filepath.Join("testdata", "completed", "moveme.txt.sfv"))
	is.NoErr(err) // read all sfv file
	is.Equal(string(b), "moveme.txt 08a8ffd8")

}

func TestWriteErr(t *testing.T) {
	is := is.New(t)

	path := filepath.Join("testdata", "errorme.txt")
	is.NoErr(os.MkdirAll("testdata", 0777))

	errPath := filepath.Join("testdata", "errorme.txt.error")

	err := ioutil.WriteFile(path, []byte("data for testing"), 0777)
	is.NoErr(err)
	defer func() {
		os.Remove(path)
		os.Remove(errPath)
	}()

	f := selfdriving.File{Path: path}
	// write an error
	f.WriteErr(errors.New("error for testing"))

	// check for error file
	content, err := ioutil.ReadFile(errPath)
	is.NoErr(err)
	is.Equal("error for testing", string(content))

}

func TestReady(t *testing.T) {
	is := is.New(t)

	path := filepath.Join("testdata", "readytest.txt")
	readyPath := filepath.Join("testdata", "readytest.txt.ready")
	is.NoErr(os.MkdirAll("testdata", 0777))

	err := ioutil.WriteFile(path, []byte("data for testing"), 0777)
	is.NoErr(err)
	defer func() {
		os.Remove(path)
		os.RemoveAll(readyPath)
	}()

	f := selfdriving.File{Path: path}

	// ready
	err = f.Ready()
	is.NoErr(err)
	_, err = os.Lstat(readyPath)
	is.NoErr(err)

	// not ready
	f.NotReady()
	_, err = os.Lstat(readyPath)
	is.True(err != nil)

}
