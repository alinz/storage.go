package merkle

type BytesStack [][]byte

func (s *BytesStack) Push(value []byte) {
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

	return value
}

func (s *BytesStack) IsEmpty() bool {
	return len(*s) == 0
}

func NewBytesStack() BytesStack {
	return make([][]byte, 0)
}
