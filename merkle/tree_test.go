package merkle_test

import (
	"fmt"
	"testing"

	"github.com/alinz/storage.go/merkle"
)

func TestMerkleTree(t *testing.T) {
	tree := merkle.NewTree(nil)

	for i := 0; i < 256; i++ {
		tree.Add([]byte{byte(i)})
	}

	fmt.Println(tree)
}

func BenchmarkMerkleTree(b *testing.B) {
	tree := merkle.NewTree(nil)
	value := []byte("value")

	for i := 0; i < b.N; i++ {
		tree.Add(value)
	}
}
