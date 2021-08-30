package memory_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/alinz/hash.go"
	"github.com/alinz/storage.go"
	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go/memory"
)

func TestMemoryStorage(t *testing.T) {
	memory := memory.New()

	content := []byte("hello")
	contentSize := int64(len(content))
	expectedHashValue, _ := hash.ValueFromString("sha256-2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")

	t.Run("Add a content", func(t *testing.T) {
		hashValue, n, err := memory.Put(context.Background(), bytes.NewReader(content))
		assert.NoError(t, err)
		assert.Equal(t, contentSize, n)
		assert.Equal(t, expectedHashValue, hash.Value(hashValue))
	})

	t.Run("make sure the value exists", func(t *testing.T) {
		rc, err := memory.Get(context.TODO(), expectedHashValue)
		assert.NoError(t, err)
		assert.Equal(t, rc, io.NopCloser(bytes.NewReader(content)))
	})

	t.Run("make sure the deleted value no longer get accessed", func(t *testing.T) {
		err := memory.Remove(context.TODO(), expectedHashValue)
		assert.NoError(t, err)
		_, err = memory.Get(context.TODO(), expectedHashValue)
		assert.Error(t, err, storage.ErrNotFound)
	})
}
