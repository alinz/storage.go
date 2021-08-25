package merkle

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
)

var (
	ErrUnknownFileType = errors.New("unknown node type")
)

type FileType byte

func (ft FileType) String() string {
	switch ft {
	case MetaType:
		return "MetaType"
	case DataType:
		return "DataType"
	default:
		return "unknown type"
	}
}

var empty32Bytes = make([]byte, 32)

const (
	_ FileType = iota
	MetaType
	DataType
)

type MetaFile struct {
	left     []byte // contains 32 bytes
	right    []byte // contains 32 bytes
	readDone bool
}

func (m *MetaFile) Left() []byte {
	return m.left
}

func (m *MetaFile) Right() []byte {
	return m.right
}

func (m *MetaFile) HasLeft() bool {
	return !bytes.Equal(empty32Bytes, m.left)
}

func (m *MetaFile) HasRight() bool {
	return !bytes.Equal(empty32Bytes, m.right)
}

func (m *MetaFile) Read(b []byte) (int, error) {
	if m.readDone {
		return 0, io.EOF
	}

	n := len(b)
	if n < 65 {
		return 0, io.ErrShortBuffer
	}

	b[0] = byte(MetaType)
	copy(b[1:33], m.left)
	copy(b[33:], m.right)
	m.readDone = true

	return 65, nil
}

func (m *MetaFile) Write(b []byte) (int, error) {
	if len(b) != 65 {
		return 0, io.ErrShortWrite
	}

	if b[0] != byte(MetaType) {
		return 0, ErrUnknownFileType
	}

	copy(m.left, b[1:33])
	copy(m.right, b[33:])

	return 65, nil
}

func (m *MetaFile) Hash() []byte {
	hasher := sha256.New()
	hasher.Write([]byte{byte(MetaType)})
	hasher.Write(m.left)
	hasher.Write(m.right)
	return hasher.Sum(nil)
}

func NewMetaFile() *MetaFile {
	return &MetaFile{
		left:  make([]byte, 32),
		right: make([]byte, 32),
	}
}

type DataFile struct {
	readDone bool
	r        *bufio.Reader
}

func (d *DataFile) Read(b []byte) (int, error) {
	if !d.readDone {
		peek, err := d.r.Peek(1)
		if len(peek) == 0 || err == io.EOF {
			return 0, io.EOF
		}

		b[0] = byte(DataType)
		d.readDone = true
		return 1, nil
	}

	return d.r.Read(b)
}

func NewDataFile(r io.Reader) *DataFile {
	return &DataFile{
		r: bufio.NewReader(r),
	}
}

func DetectFileType(r io.Reader) (io.Reader, FileType, error) {
	br := bufio.NewReader(r)

	b, err := br.Peek(1)
	if err != nil {
		return nil, 0, err
	}

	FileType := FileType(b[0])

	switch FileType {
	case MetaType:
	case DataType:
	default:
		return nil, 0, ErrUnknownFileType
	}

	return br, FileType, nil
}

func ParseMetaFile(r io.Reader) (*MetaFile, error) {
	meta := NewMetaFile()
	_, err := io.Copy(meta, r)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func ParseDataFile(r io.Reader) (io.Reader, error) {
	b := []byte{0}
	n, err := r.Read(b)

	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, io.EOF
	}

	if b[0] != byte(DataType) {
		return nil, ErrUnknownFileType
	}

	return r, nil
}
