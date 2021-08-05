package merkle_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alinz/storage.go/internal/hash"
	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/local"
	"github.com/alinz/storage.go/merkle"
	"github.com/stretchr/testify/assert"
)

func TestMerkleStorage(t *testing.T) {
	tempPath, cleanup := tests.CreateTempFolder(t)
	defer cleanup()

	fmt.Printf("temp path: %s\n", tempPath)

	local := local.New(tempPath)
	storage := merkle.New(local, local, local, 9)

	content := "123456789123456789123456789"

	hashValue, n, err := storage.Put(context.Background(), strings.NewReader(content))
	assert.NoError(t, err)
	assert.NotNil(t, hashValue)
	assert.Equal(t, n, int64(len(content)))

	v := hash.Value(hashValue)

	fmt.Printf("hash value: %s\n", v.String())
	fmt.Println("")
}
