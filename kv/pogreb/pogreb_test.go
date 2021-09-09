package pogreb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/alinz/hash.go"
	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go"
	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/kv/pogreb"
)

func TestPogrebStorage(t *testing.T) {
	filepath := filepath.Join(t.TempDir(), "database")

	memory, err := pogreb.New(filepath)
	assert.NoError(t, err)

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

	t.Run("make sure only one item is return in List", func(t *testing.T) {
		next, cancel := memory.List()
		defer cancel()

		count := 0
		for {
			value, err := next(context.Background())
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			assert.NoError(t, err)
			count++
			fmt.Println(hash.Format(value))
		}

		assert.Equal(t, 1, count)
	})

	t.Run("make sure the deleted value no longer get accessed", func(t *testing.T) {
		err := memory.Remove(context.TODO(), expectedHashValue)
		assert.NoError(t, err)
		_, err = memory.Get(context.TODO(), expectedHashValue)
		assert.Error(t, err, storage.ErrNotFound)
	})

	t.Run("put large values to the database", func(t *testing.T) {
		content := make([]byte, 1024*1024)
		for i := 0; i < len(content); i++ {
			content[i] = byte(i % 256)
		}

		hashValue, n, err := memory.Put(context.Background(), bytes.NewReader(content))
		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), n)

		rc, err := memory.Get(context.TODO(), hashValue)
		assert.NoError(t, err)
		assert.NoError(t, tests.EqualReaders(bytes.NewReader(content), rc))
	})
}
