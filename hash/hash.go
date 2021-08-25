package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"
)

const (
	hashName   = "sha256"
	hashHeader = hashName + "-"
)

type Value []byte

func (v *Value) String() string {
	return fmt.Sprintf("%s-%x", hashName, []byte(*v))
}

func (v *Value) Short() string {
	val := v.String()
	return val[len(val)-5:]
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

func ParseValueFromString(value string) (Value, error) {
	return hex.DecodeString(strings.Replace(value, hashHeader, "", 1))
}

func Print(hash []byte, w io.Writer, values ...interface{}) {
	value := Value(hash)
	values = append([]interface{}{value.Short()}, values...)
	fmt.Fprintln(w, values...)
}

func Format(value []byte) string {
	if value == nil {
		return "nil"
	}

	v := Value(value)
	return v.String()
}
