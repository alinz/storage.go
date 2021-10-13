package sqlite_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alinz/hash.go"
	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go"
	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/sqlite"
)

func TestSqlitePut(t *testing.T) {
	// dbPath := filepath.Join(t.TempDir(), "test.db")
	// backend, err := sqlite.NewFile(dbPath, 2, 1024*1024)

	backend, err := sqlite.NewMemory(2, 1024*1024)
	assert.NoError(t, err)
	defer backend.Close()

	content := []byte("hello world")
	expectedHashValue, err := hash.ValueFromString("sha256-b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
	assert.NoError(t, err)

	hashValue, n, err := backend.Put(context.TODO(), bytes.NewReader(content))
	fmt.Println(hash.Format(hashValue))
	assert.NoError(t, err)
	assert.Equal(t, n, int64(len(content)))
	assert.Equal(t, expectedHashValue, hash.Value(hashValue))

	rc, err := backend.Get(context.TODO(), hash.Value(hashValue))
	assert.NoError(t, err)
	assert.NoError(t, tests.EqualReaders(bytes.NewReader(content), rc))
	rc.Close()

	next, cancel := backend.List()
	defer cancel()
	count := 0

	for {
		hashValue, err = next(context.Background())
		if errors.Is(err, storage.ErrIteratorDone) {
			break
		}
		assert.NoError(t, err)
		assert.Equal(t, expectedHashValue, hash.Value(hashValue))
		count++
	}
	assert.Equal(t, 1, count)

	err = backend.Remove(context.Background(), expectedHashValue)
	assert.NoError(t, err)

	rc, err = backend.Get(context.TODO(), expectedHashValue)
	assert.ErrorIs(t, err, storage.ErrNotFound)
	assert.Nil(t, rc)
}
