package storage

import (
	"context"
	"errors"
	"io"
)

var (
	ErrNotFound = errors.New("not found")
)

type Putter interface {
	Put(ctx context.Context, r io.Reader) ([]byte, int64, error)
}

type Getter interface {
	Get(ctx context.Context, hash []byte) (io.ReadCloser, error)
}

type Remover interface {
	Remove(ctx context.Context, hash []byte) error
}

type Lister interface {
	List() (IteratorFunc, CancelFunc)
}
