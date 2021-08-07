package merkle

import (
	"bytes"
	"fmt"
	"io"
)

// for more infi,
// please look into the following diagram
// https://excalidraw.com/#json=6196713521938432,ktPKmwIweKVouqHgQCCJjA

type ChainFunc func(parent, child []byte, isData bool, side BranchSide) ([]byte, error)

type BranchSide int

const (
	_ BranchSide = iota
	LeftSide
	RightSide
)

type Node struct {
	Left   *Node
	Right  *Node
	height int64

	Value []byte
}

type Tree struct {
	stack     []*Node
	chainFunc ChainFunc
}

func (t *Tree) stackPush(node *Node) {
	t.stack = append(t.stack, node)
}

func (t *Tree) stackPop() *Node {
	n := len(t.stack)
	if n == 0 {
		return nil
	}
	index := n - 1
	node := t.stack[index]
	t.stack = t.stack[:index]

	return node
}

func (t *Tree) Add(value []byte) error {
	var current *Node
	var drillRequired bool
	var err error

	for {
		current = t.stackPop()

		bothFull := current.Left != nil && current.Right != nil
		stackEmpty := len(t.stack) == 0
		rightOnlyEmpty := current.Left != nil && current.Right == nil

		if bothFull && stackEmpty {
			// grow the tree
			newCurrent := &Node{
				Left:   current,
				height: current.height + 1,
			}
			t.stackPush(newCurrent)
			newCurrent.Value, err = t.chainFunc(nil, current.Value, false, LeftSide)
			if err != nil {
				return err
			}
			drillRequired = true
			continue
		}

		if bothFull {
			// need to check the parent
			continue
		}

		// create one Right node
		// and then just Left nodes until reaches height 1
		if drillRequired {
			drillRequired = false

			current.Right = &Node{
				height: current.height - 1,
			}
			t.stackPush(current)

			current = current.Right
			for current.height != 1 {
				current.Left = &Node{
					height: current.height - 1,
				}
				t.stackPush(current)
				current = current.Left
			}
			t.stackPush(current)
			continue
		}

		if rightOnlyEmpty && current.height != 1 {
			drillRequired = true
			t.stackPush(current)
			continue
		}

		if current.height == 1 {
			break
		}
	}

	dataNode := &Node{
		Value: value,
	}

	var side BranchSide
	if current.Left == nil {
		current.Left = dataNode
		side = LeftSide
	} else if current.Right == nil {
		current.Right = dataNode
		side = RightSide
	}

	t.stackPush(current)

	if t.chainFunc != nil {
		err = t.callChains(dataNode.Value, side)
	}
	return err
}

func (t *Tree) callChains(value []byte, side BranchSide) (err error) {
	isData := true
	for i := len(t.stack) - 1; i >= 0; i-- {
		current := t.stack[i]
		value, err = t.chainFunc(current.Value, value, isData, side)
		if err != nil {
			return err
		}
		current.Value = value
		isData = false
		side = RightSide
	}
	return
}

func (t *Tree) lastValue() []byte {
	return t.stack[0].Value
}

func (t *Tree) String() string {
	var buffer bytes.Buffer
	print2DUtil(t.stack[0], 0, &buffer)
	return buffer.String()
}

func NewTree(chainFunc ChainFunc) *Tree {
	return &Tree{
		chainFunc: chainFunc,
		stack: []*Node{
			{height: 1},
		},
	}
}

func print2DUtil(node *Node, space int, w io.Writer) {
	const COUNT = 5

	if node == nil {
		return
	}

	// Increase distance between levels
	space += COUNT

	// Process right child first
	print2DUtil(node.Right, space, w)

	// Print current node after space
	// count
	fmt.Fprint(w, "\n")
	for i := COUNT; i < space; i++ {
		fmt.Fprint(w, "  ")
	}

	fmt.Fprintf(w, "%d\n", node.Value)

	print2DUtil(node.Left, space, w)
}
