package tests

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

func EqualReaders(r1, r2 io.Reader) error {
	buf1 := bufio.NewReader(r1)
	buf2 := bufio.NewReader(r2)
	for {
		b1, err1 := buf1.ReadByte()
		b2, err2 := buf2.ReadByte()
		if err1 != nil && err1 != io.EOF {
			return fmt.Errorf("stream one has an error: %w", err1)
		}
		if err2 != nil && err2 != io.EOF {
			return fmt.Errorf("stream two has an error: %w", err1)
		}
		if err1 == io.EOF || err2 == io.EOF {
			if err1 != err2 {
				return fmt.Errorf("stream has a different error: %s == %s", err1, err2)
			}
			return nil
		}
		if b1 != b2 {
			return fmt.Errorf("different values %d != %d", b1, b2)
		}
	}
}

func CountFiles(t *testing.T, path string) int {
	i := 0
	files, err := ioutil.ReadDir(path)
	if err != nil {
		t.Error(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			i++
		}
	}
	return i
}
