package tests

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func CreateTempFolder(t *testing.T) (string, func()) {
	tempPath, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	return tempPath, func() {
		os.RemoveAll(tempPath)
	}
}

func EqualReaders(r1, r2 io.Reader) (identical bool) {
	buf1 := bufio.NewReader(r1)
	buf2 := bufio.NewReader(r2)
	for {
		b1, err1 := buf1.ReadByte()
		b2, err2 := buf2.ReadByte()
		if err1 != nil && err1 != io.EOF {
			return false
		}
		if err2 != nil && err2 != io.EOF {
			return false
		}
		if err1 == io.EOF || err2 == io.EOF {
			return err1 == err2
		}
		if b1 != b2 {
			return false
		}
	}
}
