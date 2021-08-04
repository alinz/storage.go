package merkle_test

import (
	"fmt"
	"testing"

	"github.com/alinz/storage.go/merkle"
)

func TestMerkleTree(t *testing.T) {
	tree := merkle.NewTree()

	for i := 1; i < 1_000_000; i++ {
		tree.Add(&merkle.Node{Value: i})
	}

	fmt.Println(tree)
}

func BenchmarkMerkleTree(b *testing.B) {
	tree := merkle.NewTree()
	node := &merkle.Node{}

	for i := 0; i < b.N; i++ {
		tree.Add(node)
	}
}
