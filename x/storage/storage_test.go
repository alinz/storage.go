package storage_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go"
	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/kv/pogreb"
	"github.com/alinz/storage.go/merkle"
	"github.com/alinz/storage.go/sqlite"
)

func TestMerkleWithPogreb(t *testing.T) {
	filepath := filepath.Join(t.TempDir(), "test.db")
	blockSize := int64(1024)

	backend, err := pogreb.New(filepath)
	assert.NoError(t, err)

	merkleStorage := merkle.New(backend, backend, backend, blockSize)

	var hashValue []byte

	t.Run("add a new content and check if it can be retrived", func(t *testing.T) {
		content := []byte("hello world")
		hash, n, err := merkleStorage.Put(context.Background(), bytes.NewReader(content))

		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), n)

		rc, err := merkleStorage.Get(context.Background(), hash)
		assert.NoError(t, err)
		assert.NoError(t, tests.EqualReaders(bytes.NewReader(content), rc))

		hashValue = hash
	})

	t.Run("listing the just added content", func(t *testing.T) {
		next, cancel := merkleStorage.List()
		defer cancel()

		for {
			hash, err := next(context.Background())
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			assert.Equal(t, hashValue, hash)
		}
	})
}

func TestMerkleWithSqlite(t *testing.T) {
	blockSize := int64(1 * 1024 * 1024)

	backend, err := sqlite.NewMemory(10, blockSize)
	assert.NoError(t, err)

	merkleStorage := merkle.New(backend, backend, backend, blockSize)

	var hashValue []byte

	t.Run("add a new content and check if it can be retrived", func(t *testing.T) {
		content := []byte("hello world")
		hash, n, err := merkleStorage.Put(context.Background(), bytes.NewReader(content))

		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), n)

		rc, err := merkleStorage.Get(context.Background(), hash)
		assert.NoError(t, err)
		assert.NoError(t, tests.EqualReaders(bytes.NewReader(content), rc))

		hashValue = hash
	})

	t.Run("listing the just added content", func(t *testing.T) {
		next, cancel := merkleStorage.List()
		defer cancel()

		for {
			hash, err := next(context.Background())
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			assert.Equal(t, hashValue, hash)
		}
	})
}
