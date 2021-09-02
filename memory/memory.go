package memory

import (
	"bytes"
	"context"
	"io"

	"github.com/alinz/hash.go"

	"github.com/alinz/storage.go"
)

type Storage struct {
	keyValue map[string][]byte
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)
var _ storage.Remover = (*Storage)(nil)
var _ storage.Lister = (*Storage)(nil)

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	hr := hash.NewReader(r)

	buffer := bytes.Buffer{}

	n, err := io.Copy(&buffer, hr)
	if err != nil {
		return nil, 0, err
	}

	hashValue := hr.Hash()

	s.keyValue[hash.Format(hashValue)] = buffer.Bytes()

	return hashValue, n, nil
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	value, ok := s.keyValue[hash.Format(hashValue)]
	if !ok {
		return nil, storage.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(value)), nil
}

func (s *Storage) Remove(ctx context.Context, hashValue []byte) error {
	key := hash.Format(hashValue)
	if _, ok := s.keyValue[key]; !ok {
		return storage.ErrNotFound
	}

	delete(s.keyValue, key)

	return nil
}

func (s *Storage) List() (storage.IteratorFunc, storage.CancelFunc) {
	mapper := func(yield storage.YieldFunc) {
		for key := range s.keyValue {
			hashValue, err := hash.ValueFromString(key)
			if err != nil {
				yield(nil, err)
				return
			}

			if ok := yield(hashValue, err); !ok {
				return
			}
		}
	}

	return storage.Iterator(mapper)
}

func New() *Storage {
	return &Storage{
		keyValue: make(map[string][]byte),
	}
}
