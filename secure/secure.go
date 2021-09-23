package secure

import (
	"context"
	"io"

	"github.com/alinz/crypto.go"
	"github.com/alinz/hash.go"

	"github.com/alinz/storage.go"
)

// Storage implements Encryption and Decryption of content into the storage
// it only support storage.Getter and storage.Putter as deletion and listing
// does not require encryption
type Storage struct {
	putter    storage.Putter
	getter    storage.Getter
	secretKey []byte
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	r, err := crypto.NewChaCha20Stream(r, s.secretKey)
	if err != nil {
		return nil, 0, err
	}

	return s.putter.Put(ctx, r)
}

func (s *Storage) Get(ctx context.Context, hash []byte) (io.ReadCloser, error) {
	rc, err := s.getter.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	return crypto.NewChaCha20Stream(rc, s.secretKey)
}

func New(putter storage.Putter, getter storage.Getter, secretKey []byte) *Storage {
	return &Storage{
		putter:    putter,
		getter:    getter,
		secretKey: hash.Bytes(secretKey),
	}
}
