package local_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alinz/hash.go"

	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/local"
	"github.com/stretchr/testify/assert"
)

func TestLocalStorage(t *testing.T) {
	tempPath := t.TempDir()

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
		assert.NoError(t, tests.EqualReaders(contentReader, r))
	})
}
