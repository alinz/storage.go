package secure_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go/memory"
	"github.com/alinz/storage.go/secure"
)

func TestSecureStorage(t *testing.T) {
	var secureStorage *secure.Storage

	memoryStorage := memory.New()
	secureStorage = secure.New(memoryStorage, memoryStorage, []byte("my secret key"))

	expectedContent := []byte("hello world")
	expectedCiphertextSize := int64(len(expectedContent))

	hashValue, n, err := secureStorage.Put(context.Background(), bytes.NewReader(expectedContent))
	assert.NoError(t, err)
	assert.Equal(t, expectedCiphertextSize, n)

	plaintextReader, err := secureStorage.Get(context.Background(), hashValue)
	assert.NoError(t, err)
	defer plaintextReader.Close()
	plaintext, err := io.ReadAll(plaintextReader)
	assert.NoError(t, err)
	assert.Equal(t, expectedContent, plaintext)

	secureStorage = secure.New(memoryStorage, memoryStorage, []byte("my secret key 1234"))
	plaintextReader, err = secureStorage.Get(context.Background(), hashValue)
	assert.NoError(t, err)
	defer plaintextReader.Close()

	// reading content storage with wrong key, returns wrong cotent
	// no error should be transmited, usually stream cipher do not support check
	b, err := ioutil.ReadAll(plaintextReader)
	assert.NoError(t, err)
	assert.NotEqual(t, expectedContent, b)
}
