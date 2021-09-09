package boltdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/alinz/hash.go"
	"github.com/boltdb/bolt"

	"github.com/alinz/storage.go"
)

var bucketName = []byte("data")

type Storage struct {
	db *bolt.DB
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

	if n == 0 {
		return nil, 0, io.EOF
	}

	hashValue := hr.Hash()

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if err != nil {
			return err
		}

		return b.Put(hashValue, buffer.Bytes())
	})

	return hashValue, n, err
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	var buffer bytes.Buffer

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New("bucket not found")
		}

		value := b.Get(hashValue)
		if value == nil {
			return storage.ErrNotFound
		}

		_, err := buffer.Write(value)
		return err
	})
	if err != nil {
		return nil, err
	}

	return io.NopCloser(&buffer), nil
}

func (s *Storage) Remove(ctx context.Context, hashValue []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.Delete(hashValue)
	})
}

func (s *Storage) List() (storage.IteratorFunc, storage.CancelFunc) {
	mapper := func(yield storage.YieldFunc) {
		s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			c := b.Cursor()

			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if ok := yield(k, nil); !ok {
					break
				}
			}

			return nil
		})
	}

	return storage.Iterator(mapper)
}

func New(filepath string) (*Storage, error) {
	db, err := bolt.Open(filepath, os.ModePerm, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(t *bolt.Tx) error {
		_, err := t.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &Storage{db: db}, nil
}
