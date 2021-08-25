package hash_test

import (
	"testing"

	"github.com/alinz/storage.go/hash"
	"github.com/stretchr/testify/assert"
)

func TestValueStringConverstion(t *testing.T) {
	expectedHashValue := "sha256-2498ad992b02c2f6e21684e8057a01463acad5c75a4e75d095619c556a559e8c"
	hashValue, err := hash.ParseValueFromString(expectedHashValue)
	assert.NoError(t, err)
	assert.Equal(t, expectedHashValue, hashValue.String())
}
