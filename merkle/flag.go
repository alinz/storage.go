package merkle

import (
	"errors"
	"io"
)

var (
	ErrUnknownFlag = errors.New("unknown flag")
)

type Flag byte

const (
	_ Flag = iota
	NodeFlag
	DataFlag
)

type flagAppendReader struct {
	flag Flag
	r    io.Reader
}

func (f *flagAppendReader) Read(b []byte) (int, error) {
	if f.flag != 0 {
		b[0] = byte(f.flag)
		f.flag = 0
		return 1, nil
	}

	return f.Read(b)
}

func AppendFlagReader(r io.Reader, flag Flag) io.Reader {
	return &flagAppendReader{flag: flag, r: r}
}

func ParseFlag(r io.Reader) (io.Reader, Flag, error) {
	b := []byte{0}
	n, err := r.Read(b)
	if err != nil {
		return nil, 0, err
	} else if n == 0 {
		return nil, 0, ErrUnknownFlag
	}

	flag := Flag(b[0])
	if flag != NodeFlag && flag != DataFlag {
		return nil, 0, ErrUnknownFlag
	}

	return r, flag, nil
}
