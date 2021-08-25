package merkle

import (
	"fmt"

	"github.com/alinz/storage.go/internal/hash"
)

type BytesStack [][]byte

func (s *BytesStack) Push(value []byte) {
	fmt.Println("PUSH <- ", hash.Format(value))
	*s = append(*s, value)
}

func (s *BytesStack) Pop() []byte {
	n := len(*s)
	if n == 0 {
		return nil
	}
	index := n - 1
	value := (*s)[index]
	*s = (*s)[:index]

	fmt.Println("POP -> ", hash.Format(value))

	return value
}

func (s *BytesStack) IsEmpty() bool {
	return len(*s) == 0
}

func NewBytesStack() BytesStack {
	return make([][]byte, 0)
}
