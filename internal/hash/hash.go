package hash

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
)

const (
	hashName         = "sha256"
	hashHeader       = hashName + "-"
	hashHeaderLength = len(hashHeader)
	doubleQuote      = "\""
)

type Value []byte

func (v *Value) String() string {
	return fmt.Sprintf("%s-%x", hashName, []byte(*v))
}

func (v *Value) MarshalJSON() ([]byte, error) {
	var buffer bytes.Buffer

	buffer.WriteString(doubleQuote)
	buffer.WriteString(v.String())
	buffer.WriteString(doubleQuote)

	return buffer.Bytes(), nil
}

func (v *Value) UnmarshalJSON(data []byte) error {
	// need to remove quotes from data
	data = bytes.Trim(data, doubleQuote)

	if !bytes.HasPrefix(data, []byte(hashHeader)) {
		return fmt.Errorf("wrong format")
	}

	*v = bytes.Trim(data, hashHeader)
	return nil
}

func Bytes(value []byte) Value {
	hasher := sha256.New()
	hasher.Write(value)
	return hasher.Sum(nil)
}

type Reader struct {
	r      io.Reader
	hasher hash.Hash
}

func (r *Reader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	if n > 0 {
		r.hasher.Write(b[:n])
	} else if n == 0 {
		return 0, io.EOF
	}

	return n, err
}

func (r Reader) Hash() Value {
	return r.hasher.Sum(nil)
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:      r,
		hasher: sha256.New(),
	}
}
