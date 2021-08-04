package local_test

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/alinz/storage.go/internal/hash"
	"github.com/alinz/storage.go/local"
	"github.com/stretchr/testify/assert"
)

func createTempFolder(t *testing.T) (string, func()) {
	tempPath, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	return tempPath, func() {
		os.RemoveAll(tempPath)
	}
}

func diffReaders(r1, r2 io.Reader) (identical bool) {
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

func TestLocalStorage(t *testing.T) {
	tempPath, cleanupTempPath := createTempFolder(t)
	defer cleanupTempPath()

	local := local.New(tempPath)

	content := "hello world"
	actualHash := hash.Bytes([]byte(content))
	actualSize := int64(len(content))

	t.Run("should be able to save a content and retrive it using the same hash", func(t *testing.T) {
		contentReader := strings.NewReader(content)

		hashValue, size, err := local.Put(context.TODO(), contentReader)
		assert.NoError(t, err)
		assert.Equal(t, size, actualSize)
		assert.Equal(t, hash.Value(hashValue), actualHash)

		r, err := local.Get(context.TODO(), actualHash)
		assert.NoError(t, err)
		defer r.Close()

		contentReader.Reset(content)
		assert.True(t, diffReaders(contentReader, r))
	})
}
