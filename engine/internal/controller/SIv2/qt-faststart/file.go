// forked from https://github.com/DejaMi/go-qt-faststart

package qtfaststart

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
)

type reader struct {
	rd   *readerWrapper
	skip []Atom
}

// NewReader returns an io.Reader which rearranges the stream from src such that the "moov" atom is as
// close as possible to the beginning of the file.
func NewReader(r io.Reader, file *File, clean bool) (io.Reader, error) {
	var shiftBy uint64

	if err := file.validate(); err != nil {
		return nil, fmt.Errorf("invalid atoms: %s", err)
	}

	rw := &readerWrapper{
		rd: io.MultiReader(
			bytes.NewReader(file.ftyp.Bytes),
			bytes.NewReader(file.moov.Bytes),
			r,
		),
	}
	rd := &reader{rd: rw}

	if file.moov.Offset > file.mdat.Offset {
		shiftBy += file.moov.Size
	}

	// skip byte offset ranges for moved/removed atoms
	for _, atom := range file.atoms {
		switch atom.Type {
		case "ftyp", "moov":
		case "free", "\x00\x00\x00\x00":
			if !clean {
				continue
			}
			if atom.Offset < file.mdat.Offset {
				shiftBy += atom.Size
			}
		default:
			continue
		}

		// adjust offsets by size of ftyp and moov atoms, since those bytes will copied to the beginning
		atom.Offset += file.ftyp.Size + file.moov.Size
		rd.skip = append(rd.skip, atom)
	}

	err := patchChunkOffsetAtoms(&file.moov, shiftBy)
	if err != nil {
		return nil, err
	}

	return rd, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	var nr int
	l := len(p)

	// discard bytes and advance reader if buffer overlaps skipped atoms
	for _, atom := range r.skip {
		start, end := r.rd.offset, r.rd.offset+uint64(l-n)

		if start < atom.End() && end > atom.Offset {
			if s := atom.Offset - start; s > 0 {
				nr, err = r.rd.Read(p[n : n+int(s)]) // read bytes until start of skipped atom
				n += nr
				if err != nil {
					return
				}
			}

			// discard bytes from current offset to the end of the skipped atom
			_, err = io.CopyN(ioutil.Discard, r.rd, int64(atom.End()-r.rd.offset))
			if err != nil {
				return
			}
		}
	}

	// read the remaining bytes, if any
	if n < l {
		nr, err = r.rd.Read(p[n:])
		n += nr
	}

	return
}

type readerWrapper struct {
	rd     io.Reader
	offset uint64
}

func (r *readerWrapper) Read(p []byte) (n int, err error) {
	var nr int
	l := len(p)

	// read until the buffer is full, or an error occurs
	for {
		nr, err = r.rd.Read(p[n:])
		r.offset += uint64(nr)
		if err != nil {
			return
		}
		n += nr

		if n >= l {
			return
		}

		// TODO fail after max consecutive zero reads
	}
}

// A File represents a Quicktime movie file.
type File struct {
	atoms []Atom
	ftyp  Atom
	mdat  Atom
	moov  Atom
}

// Parse creates and initializes a File using data read from an io.streamio.
func Parse(reader io.Reader) (*File, error) {
	f := new(File)
	if err := f.parse(reader); err != nil {
		return nil, err
	}
	if err := f.validate(); err != nil {
		return nil, err
	}
	return f, nil
}

// FastStartEnabled returns true if the "moov" atom is already at the beginning
// of the file.
func (f *File) FastStartEnabled() bool {
	return f.moov.Offset < f.mdat.Offset
}

func (f *File) parse(r io.Reader) error {
	var offset uint64
	r = &readerWrapper{rd: r}

	for {
		if atom, err := readNextAtom(r); err == nil {
			if atom.IsZero() {
				continue
			}

			atom.Offset = offset
			offset += atom.Size
			f.atoms = append(f.atoms, atom)
			switch atom.Type {
			case "ftyp":
				f.ftyp = atom
			case "mdat":
				f.mdat = atom
			case "moov":
				f.moov = atom
			case "free", "junk", "pict", "pnot", "skip", "uuid", "wide", "sidx",
				"moof", "pdin", "mfra", "tfra", "meta", "meco", "styp", "ssix", "prft":
				// Do nothing
			default:
				return fmt.Errorf("%q is not a valid top-level atom", atom.Type)
			}
			if f.mdat.Type == "mdat" && f.moov.Type == "moov" {
				if f.FastStartEnabled() {
					return nil
				}
			}
		} else if err == io.EOF {
			break
		} else {
			return err
		}
	}
	return nil
}

func (f *File) validate() error {
	if f.ftyp.IsZero() {
		return errors.New("Invalid file: ftyp atom not found")
	}
	if f.mdat.IsZero() {
		return errors.New("Invalid file: mdat atom not found")
	}
	if f.moov.IsZero() {
		return errors.New("Invalid file: moov atom not found")
	}
	if yes, err := containsCompressedAtoms(f.moov); err != nil {
		return err
	} else if yes {
		return errors.New("Compressed moov atoms are not supported")
	}
	return nil
}

func containsCompressedAtoms(parent Atom) (bool, error) {
	offset := parent.Offset
	for offset < parent.Size {
		br := bytes.NewReader(parent.Bytes)
		if child, err := readNextAtom(br); err == nil {
			if child.Type == "cmov" && !child.IsZero() {
				return true, nil
			}

			offset += child.Size
		} else {
			return false, err
		}
	}
	return false, nil
}

func readNextAtom(r io.Reader) (Atom, error) {
	atom := Atom{HeaderSize: 8}
	header := make([]byte, 16)

	// read the first 8 bytes
	_, err := r.Read(header[:8])
	if err != nil {
		if err == io.EOF {
			return atom, err
		}

		return atom, fmt.Errorf("failed to read atom header: %s", err)
	}

	// The first 4 bytes contain the size of the atom
	atom.Size = uint64(binary.BigEndian.Uint32(header[0:4]))

	// The next 4 bytes contain the type of the atom
	atom.Type = string(header[4:8])

	// If the size is 1, look at the next 8 bytes for the real size
	if atom.Size == 1 {
		if _, err := r.Read(header[8:16]); err != nil {
			return atom, fmt.Errorf("failed to read atom header: %s", err)
		}

		atom.Size = binary.BigEndian.Uint64(header[8:16])
		atom.HeaderSize += 8
	}

	// Make sure the atom is at least as large as its header
	if atom.Size > 0 && atom.Size < uint64(atom.HeaderSize) {
		return atom, fmt.Errorf("Invalid file format: Atom %q at offset %d reported a size of only %d bytes", atom.Type, atom.Offset, atom.Size)
	}

	bytesToRead := int64(atom.Size) - int64(atom.HeaderSize)
	if bytesToRead > 0 {
		// Only capture non-mdat atom bytes
		if atom.Type != "mdat" {
			atom.Bytes = make([]byte, atom.Size)
			copy(atom.Bytes, header[:atom.HeaderSize])

			// Read everything after the header
			n, err := r.Read(atom.Bytes[atom.HeaderSize:])
			if err != nil {
				if err == io.EOF {
					return atom, err
				}
				return atom, fmt.Errorf("failed to read atom body: %s", err)
			}

			log.Printf("read %d of %d bytes of %q atom", n, bytesToRead, atom.Type)
		} else {
			// read and discard mdat atom
			n, err := io.CopyN(ioutil.Discard, r, bytesToRead)
			if err != nil {
				return atom, fmt.Errorf("failed to read mdat atom: %s", err)
			}

			log.Printf("discarded %d bytes of mdat atom", n)
		}
	}

	return atom, nil
}

func patchChunkOffsetAtoms(parent *Atom, shiftBy uint64) error {
	atoms, err := findChunkOffsetAtoms(*parent)
	if err != nil {
		return err
	}

	for _, atom := range atoms {
		// Ignore the first 4 bytes after the header
		// The next 4 bytes tells us how many entries need to be patched
		offset := atom.HeaderEnd() - parent.Offset + 8
		numEntries := uint64(binary.BigEndian.Uint32(parent.Bytes[offset-4 : offset]))

		var i uint64
		if atom.Type == "stco" {
			for i = 0; i < numEntries; i++ {
				start, end := offset+4*i, offset+4*(i+1)
				entryBytes := parent.Bytes[start:end]
				entry := binary.BigEndian.Uint32(entryBytes)
				entry += uint32(shiftBy)
				binary.BigEndian.PutUint32(entryBytes, entry)
			}
		} else if atom.Type == "co64" {
			for i = 0; i < numEntries; i++ {
				start, end := offset+8*i, offset+8*(i+1)
				entryBytes := parent.Bytes[start:end]
				entry := binary.BigEndian.Uint64(entryBytes)
				entry += shiftBy
				binary.BigEndian.PutUint64(entryBytes, entry)
			}
		}
	}

	return nil
}

func findChunkOffsetAtoms(parent Atom) ([]Atom, error) {
	var err error

	if len(parent.Bytes) <= int(parent.HeaderSize) {
		return nil, errors.New("atom body is empty")
	}

	atoms := []Atom{}
	offset := uint64(parent.HeaderSize)
	buf := bytes.NewReader(parent.Bytes[offset:])

	for offset < parent.Size {
		child, err := readNextAtom(buf)
		if err == nil && !child.IsZero() {
			child.Offset = parent.Offset + offset

			switch child.Type {
			case "stco", "co64":
				atoms = append(atoms, child)
			case "trak", "mdia", "minf", "stbl": // These can have chunk offset atoms as children
				children, err := findChunkOffsetAtoms(child)
				if err != nil {
					return nil, err
				}

				atoms = append(atoms, children...)
			}
			offset += child.Size
		}
	}
	return atoms, err
}
