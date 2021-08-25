package merkle_test

import (
	"testing"

	"github.com/alinz/storage.go/merkle"
	"github.com/stretchr/testify/assert"
)

func TestMerkleTreeCheckCallbackCalls(t *testing.T) {
	testCases := []struct {
		inserted int
		numCalls int
	}{
		{
			inserted: 1,
			numCalls: 1,
		},
		{
			inserted: 2,
			numCalls: 2,
		},
		{
			inserted: 3,
			numCalls: 5,
		},
		{
			inserted: 4,
			numCalls: 7,
		},
		{
			inserted: 5,
			numCalls: 11,
		},
		{
			inserted: 6,
			numCalls: 14,
		},
	}

	for _, testCase := range testCases {
		total := 0

		tree := merkle.NewTree(func(parent, child []byte, side merkle.NodeSide) ([]byte, error) {
			total++
			return nil, nil
		})

		for i := 0; i < testCase.inserted; i++ {
			tree.Add([]byte{1})
		}

		assert.Equal(t, testCase.numCalls, total)
	}
}

func TestMerkleTreeCallbackSides(t *testing.T) {
	testCases := []struct {
		n        int
		children []merkle.NodeSide
	}{
		{
			n: 1,
			children: []merkle.NodeSide{
				merkle.LeftSide,
			},
		},
		{
			n: 2,
			children: []merkle.NodeSide{
				merkle.LeftSide,
				merkle.RightSide,
			},
		},
		{
			n: 3,
			children: []merkle.NodeSide{
				merkle.LeftSide,
				merkle.RightSide,
				merkle.LeftSide,
				merkle.LeftSide,
				merkle.RightSide,
			},
		},
		{
			n: 4,
			children: []merkle.NodeSide{
				merkle.LeftSide,
				merkle.RightSide,
				merkle.LeftSide,
				merkle.LeftSide,
				merkle.RightSide,
				merkle.RightSide,
				merkle.RightSide,
			},
		},
	}

	for _, testCase := range testCases {
		idx := 0

		tree := merkle.NewTree(func(parent, child []byte, side merkle.NodeSide) ([]byte, error) {
			assert.Equal(t, testCase.children[idx], side)
			idx++
			return nil, nil
		})

		for i := 0; i < testCase.n; i++ {
			tree.Add([]byte{1})
		}

		assert.Equal(t, idx, len(testCase.children))
	}
}

func TestMerkleTreeCallbackResults(t *testing.T) {
	testCases := []struct {
		n        byte
		parents  [][]byte
		children [][]byte
	}{
		{
			n:        0,
			parents:  [][]byte{},
			children: [][]byte{},
		},
		{
			n:        1,
			parents:  [][]byte{nil},
			children: [][]byte{{1}},
		},
		{
			n:        2,
			parents:  [][]byte{nil, {1}, {2}},
			children: [][]byte{{1}, {2}},
		},
	}

	for _, testCase := range testCases {
		idx := 0

		tree := merkle.NewTree(func(parent, child []byte, side merkle.NodeSide) ([]byte, error) {
			assert.Equal(t, parent, testCase.parents[idx])
			assert.Equal(t, child, testCase.children[idx])
			idx++

			if parent == nil {
				return child, nil
			}

			return []byte{parent[0] + child[0]}, nil
		})

		for i := byte(0); i < testCase.n; i++ {
			tree.Add([]byte{i + 1})
		}
	}
}
