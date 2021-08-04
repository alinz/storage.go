package merkle

import "io"

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
