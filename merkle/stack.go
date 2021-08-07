package merkle

type HashStack [][]byte

func (s *HashStack) Push(hash []byte) {
	*s = append(*s, hash)
}

func (s *HashStack) Pop() []byte {
	n := len(*s)
	if n == 0 {
		return nil
	}
	index := n - 1
	hash := (*s)[index]
	*s = (*s)[:index]

	return hash
}

func (s *HashStack) IsEmpty() bool {
	return len(*s) == 0
}

func NewHashStack() HashStack {
	return make([][]byte, 0)
}
