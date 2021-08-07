package merkle

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
)

type Flagger interface {
	io.ReadWriter
}

var (
	ErrUnknownNodeType = errors.New("unknown node type")
	ErrEmptyNode       = errors.New("empty node")
)

type NodeType byte

var empty32Bytes = make([]byte, 32)

const (
	_ NodeType = iota
	MetaType
	DataType
)

type MetaNode struct {
	left     []byte // contains 32 bytes
	right    []byte // contains 32 bytes
	readDone bool
}

func (m *MetaNode) HasLeft() bool {
	return !bytes.Equal(empty32Bytes, m.left)
}

func (m *MetaNode) HasRight() bool {
	return !bytes.Equal(empty32Bytes, m.right)
}

func (m *MetaNode) Read(b []byte) (int, error) {
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

func (m *MetaNode) Write(b []byte) (int, error) {
	if len(b) != 65 {
		return 0, io.ErrShortWrite
	}

	if b[0] != byte(MetaType) {
		return 0, ErrUnknownNodeType
	}

	copy(m.left, b[1:33])
	copy(m.right, b[33:])

	return 65, nil
}

func (m *MetaNode) Hash() []byte {
	hasher := sha256.New()
	hasher.Write([]byte{byte(MetaType)})
	hasher.Write(m.left)
	hasher.Write(m.right)
	return hasher.Sum(nil)
}

func NewMetaNode() *MetaNode {
	return &MetaNode{
		left:  make([]byte, 32),
		right: make([]byte, 32),
	}
}

type DataNode struct {
	readDone bool
	r        *bufio.Reader
}

func (d *DataNode) Read(b []byte) (int, error) {
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

func NewDataNode(r io.Reader) *DataNode {
	return &DataNode{
		r: bufio.NewReader(r),
	}
}

func ParseNode(r io.Reader) (io.Reader, NodeType, error) {
	br := bufio.NewReader(r)

	b, err := br.Peek(1)
	if err != nil {
		return nil, 0, err
	}

	nodeType := NodeType(b[0])

	switch nodeType {
	case MetaType:
	case DataType:
	default:
		return nil, 0, ErrUnknownNodeType
	}

	return br, nodeType, nil
}

func ParseMetaNode(r io.Reader) (*MetaNode, error) {
	meta := NewMetaNode()
	_, err := io.Copy(meta, r)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func ParseDataNode(r io.Reader) (io.Reader, error) {
	b := []byte{0}
	n, err := r.Read(b)

	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, io.EOF
	}

	if b[0] != byte(DataType) {
		return nil, ErrUnknownNodeType
	}

	return r, nil
}
