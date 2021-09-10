package pogreb

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/akrylysov/pogreb"
	"github.com/alinz/hash.go"
	"github.com/alinz/storage.go"
)

type Storage struct {
	db *pogreb.DB
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)
var _ storage.Remover = (*Storage)(nil)
var _ storage.Lister = (*Storage)(nil)
var _ storage.Closer = (*Storage)(nil)

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	hr := hash.NewReader(r)

	buffer := bytes.Buffer{}

	n, err := io.Copy(&buffer, hr)
	if err != nil {
		return nil, 0, err
	}

	if n == 0 {
		return nil, 0, io.EOF
	}

	hashValue := hr.Hash()

	err = s.db.Put(hashValue, buffer.Bytes())
	if err != nil {
		return nil, 0, err
	}

	return hashValue, n, nil
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	value, err := s.db.Get(hashValue)
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, storage.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(value)), nil
}

func (s *Storage) Remove(ctx context.Context, hashValue []byte) error {
	return s.db.Delete(hashValue)
}

func (s *Storage) List() (storage.IteratorFunc, storage.CancelFunc) {
	it := s.db.Items()

	mapper := func(yield storage.YieldFunc) {
		for {
			key, _, err := it.Next()
			if errors.Is(err, pogreb.ErrIterationDone) {
				return
			}

			if ok := yield(key, err); !ok {
				return
			}
		}
	}

	return storage.Iterator(mapper)
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func New(filepath string) (*Storage, error) {
	db, err := pogreb.Open(filepath, nil)
	if err != nil {
		return nil, err
	}

	return &Storage{db: db}, nil
}
