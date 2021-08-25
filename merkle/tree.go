package merkle

import (
	"bytes"
	"fmt"
	"io"
)

type Callback func(parent []byte, child []byte, side NodeSide) ([]byte, error)

// for more infi,
// please look into the following diagram
// https://excalidraw.com/#json=6196713521938432,ktPKmwIweKVouqHgQCCJjA

type NodeSide byte

func (ns NodeSide) String() string {
	switch ns {
	case LeftSide:
		return "L"
	case RightSide:
		return "R"
	default:
		return "?" // means node is ROOT
	}
}

const (
	_ NodeSide = iota
	LeftSide
	RightSide
)

type Node struct {
	Left   *Node
	Right  *Node
	height int64
	side   NodeSide

	Value []byte
}

type Tree struct {
	callback Callback
	stack    []*Node
}

func (t *Tree) root() *Node {
	return t.stack[0]
}

func (t *Tree) push(node *Node) {
	t.stack = append(t.stack, node)
}

func (t *Tree) pop() *Node {
	n := len(t.stack)
	if n == 0 {
		return nil
	}
	index := n - 1
	node := t.stack[index]
	t.stack = t.stack[:index]

	return node
}

func (t *Tree) peek() *Node {
	n := len(t.stack)
	if n == 0 {
		return nil
	}

	return t.stack[n-1]
}

func (t *Tree) Add(value []byte) error {
	err := t.grow()
	if err != nil {
		return err
	}

	current := t.peek()

	newNode := &Node{
		height: current.height - 1,
		Value:  value,
	}

	if current.Left == nil {
		newNode.side = LeftSide
		current.Left = newNode
	} else if current.Right == nil {
		newNode.side = RightSide
		current.Right = newNode
	}

	return t.callCallback(newNode)
}

func (t *Tree) grow() (err error) {
	for {
		current := t.pop()
		isCurrentData := current.height == 1
		noMoreNodes := len(t.stack) == 0

		if (current.Left == nil || current.Right == nil) && isCurrentData {
			t.push(current)
			break
		}

		if noMoreNodes && current.Right != nil {
			current.side = LeftSide

			t.push(&Node{
				height: current.height + 1,
				Left:   current,
			})

			err = t.callCallback(current)
			if err != nil {
				return err
			}

			continue
		}

		if current.Right == nil {
			// need to create one right
			// and drill down all left to reach height == 1
			doneRight := true
			for {
				t.push(current)

				if current.height == 1 {
					break
				}

				if doneRight {
					doneRight = false
					current.Right = &Node{
						height: current.height - 1,
						side:   RightSide,
					}
					current = current.Right
				} else {
					current.Left = &Node{
						height: current.height - 1,
						side:   LeftSide,
					}
					current = current.Left
				}
			}

			continue
		}
	}

	return
}

func (t *Tree) callCallback(child *Node) (err error) {
	n := len(t.stack) - 1

	for i := range t.stack {
		parent := t.stack[n-i]
		parent.Value, err = t.callback(parent.Value, child.Value, child.side)
		if err != nil {
			return err
		}
		child = parent
	}

	return nil
}

func (t *Tree) String() string {
	var buffer bytes.Buffer
	print2DUtil(t.stack[0], 0, &buffer)
	return buffer.String()
}

func NewTree(callback Callback) *Tree {
	return &Tree{
		callback: callback,
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

	fmt.Fprintf(w, "(%s)(%d)\n", node.side, node.Value)

	print2DUtil(node.Left, space, w)
}
