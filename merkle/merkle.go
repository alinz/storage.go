package merkle

import "github.com/alinz/storage.go"

type Storage struct {
}

var _ storage.Getter = (*Storage)(nil)
var _ storage.Putter = (*Storage)(nil)
