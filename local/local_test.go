package local_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alinz/hash.go"
	"github.com/alinz/storage.go"

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

		next, cancel := local.List()
		defer cancel()

		for {
			hashValue, err := next(context.Background())
			if err == storage.ErrIteratorDone {
				break
			} else if err != nil {
				t.Fatal(err)
			}

			fmt.Println(hash.Format(hashValue))
		}
	})
}

func TestListLargeNumberofFiles(t *testing.T) {
	tempDir := t.TempDir()
	local := local.New(tempDir)

	filesCount := 100_000

	// prepare the test by generating a lot of content/files
	for i := 0; i < filesCount; i++ {
		local.Put(context.TODO(), strings.NewReader(fmt.Sprintf("%d", i)))
	}

	fmt.Printf("Completed generating %d files\n", filesCount)

	// create the storage
	next, cancel := local.List()
	defer cancel()

	count := 0

	for {
		_, err := next(context.Background())
		if err == storage.ErrIteratorDone {
			break
		}

		count++
		if count%10_000 == 0 {
			fmt.Printf("Procced %d files\n", count)
		}
		assert.NoError(t, err)
	}

	assert.Equal(t, count, 100_000)
}
