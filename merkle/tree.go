package merkle

import (
	"bytes"
	"fmt"
	"io"
)

// for more infi,
// please look into the following diagram
// https://excalidraw.com/#json=6196713521938432,ktPKmwIweKVouqHgQCCJjA

type Node struct {
	Left   *Node
	Right  *Node
	height int64

	Value int
}

type Tree struct {
	stack []*Node
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

func (t *Tree) Add(newNode *Node) {
	var current *Node
	var drillRequired bool

	for {
		current = t.stackPop()

		bothFull := current.Left != nil && current.Right != nil
		stackEmpty := len(t.stack) == 0
		rightOnlyEmpty := current.Left != nil && current.Right == nil

		if bothFull && stackEmpty {
			// grow the tree
			current = &Node{
				Left:   current,
				height: current.height + 1,
			}
			t.stackPush(current)
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

	if current.Left == nil {
		current.Left = newNode
	} else if current.Right == nil {
		current.Right = newNode
	}

	t.stackPush(current)
}

func (t *Tree) String() string {
	var buffer bytes.Buffer
	print2DUtil(t.stack[0], 0, &buffer)
	return buffer.String()
}

func NewTree() *Tree {
	return &Tree{
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
