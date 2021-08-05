package merkle

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"

	"github.com/alinz/storage.go"
)

type Meta struct {
	Left  []byte // contains 32 bytes
	Right []byte // contains 32 bytes
	done  bool
}

func newMeta() *Meta {
	return &Meta{
		Left:  make([]byte, 32),
		Right: make([]byte, 32),
	}
}

func (m *Meta) Read(b []byte) (int, error) {
	if m.done {
		return 0, io.EOF
	}

	n := len(b)
	if n < 64 {
		return 0, io.ErrShortBuffer
	}

	copy(b[:32], m.Left)
	copy(b[32:], m.Right)
	m.done = true

	return 64, nil
}

func (m *Meta) Write(b []byte) (int, error) {
	if len(b) != 64 {
		return 0, io.ErrShortWrite
	}

	copy(m.Left, b[:32])
	copy(m.Right, b[32:])

	return 64, nil
}

func (m *Meta) Hash() []byte {
	hasher := sha256.New()
	hasher.Write(m.Left)
	hasher.Write(m.Right)
	return hasher.Sum(nil)
}

type Storage struct {
	blockSize int64
	putter    storage.Putter
	getter    storage.Getter
	remover   storage.Remover
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	var totalSize int64

	tree := NewTree(s.rebalance)

	for {
		hashValue, n, err := s.putter.Put(ctx, io.LimitReader(r, s.blockSize))
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, totalSize, err
		} else if n == 0 {
			break
		}

		totalSize += n
		err = tree.Add(hashValue)
		if err != nil {
			return hashValue, totalSize, err
		}
	}

	return tree.lastValue(), totalSize, nil
}

func (s *Storage) Get(ctx context.Context, hash []byte) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		_ = pw
	}()

	return pr, nil
}

func (s *Storage) rebalance(parent, child []byte, isChildData bool, side BranchSide) ([]byte, error) {
	parentMeta, err := s.loadMeta(parent)
	if err != nil {
		return nil, err
	}

	if isChildData {
		switch side {
		case LeftSide:
			copy(parentMeta.Left, child)
		case RightSide:
			copy(parentMeta.Right, child)
		}
	} else {
		copy(parentMeta.Right, child)
	}

	err = s.remover.Remove(context.Background(), parent)
	if errors.Is(err, storage.ErrNotFound) {
		// ignore this situation, as parent might not be created yet
	} else if err != nil {
		return nil, err
	}

	newParent, _, err := s.putter.Put(context.Background(), parentMeta)
	if err != nil {
		return nil, err
	}

	return newParent, nil
}

// loadMeta tries to load Meta information from underneath storage
// if meta couldn't be loaded because of NotFound error, then an empty
// Meta will be returned
func (s *Storage) loadMeta(hash []byte) (*Meta, error) {
	r, err := s.getter.Get(context.Background(), hash)
	if errors.Is(err, storage.ErrNotFound) {
		return newMeta(), nil
	} else if err != nil {
		return nil, err
	}
	defer r.Close()

	meta := newMeta()

	_, err = io.Copy(meta, r)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func New(
	putter storage.Putter,
	getter storage.Getter,
	remover storage.Remover,
	blockSize int64,
) *Storage {
	return &Storage{
		blockSize: blockSize,
		putter:    putter,
		getter:    getter,
		remover:   remover,
	}
}
