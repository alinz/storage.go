package merkle

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/alinz/storage.go"
	"github.com/alinz/storage.go/internal/hash"
)

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
	var totalBlocks int64

	tree := NewTree(s.rebalance)

	for {
		dataNode := NewDataNode(io.LimitReader(r, s.blockSize))
		// nr := io.LimitReader(r, s.blockSize)
		hashValue, n, err := s.putter.Put(ctx, dataNode)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, totalSize, err
		} else if n == 0 {
			break
		}

		totalBlocks++
		totalSize += n
		err = tree.Add(hashValue)
		if err != nil {
			return hashValue, totalSize, err
		}
	}

	return tree.lastValue(), totalSize - totalBlocks, nil
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		stack := NewHashStack()

		walkTree := func() {
			hashValue = stack.Pop()

			hash.Print(hashValue, os.Stdout, " <- root")

			// need to load the content to detect whether is Meta or Data
			r, err := s.getter.Get(ctx, hashValue)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			defer r.Close()

			reader, nodeType, err := ParseNode(r)
			if err != nil {
				pw.CloseWithError(err)
				return
			}

			switch nodeType {
			case MetaType:
				meta, err := ParseMetaNode(reader)
				if err != nil {
					pw.CloseWithError(err)
					return
				}

				hash.Print(meta.left, os.Stdout, " <- left")
				hash.Print(meta.right, os.Stdout, " <- right")

				if meta.HasRight() {
					stack.Push(meta.right)
				}

				if meta.HasLeft() {
					stack.Push(meta.left)
				}

			case DataType:
				reader, _ = ParseDataNode(reader)
				_, err = io.Copy(pw, reader)
				if err != nil {
					pw.CloseWithError(err)
					return
				}
			}
		}

		stack.Push(hashValue)
		for !stack.IsEmpty() {
			walkTree()
		}

		pw.Close()
	}()

	return pr, nil
}

func (s *Storage) rebalance(parent, child []byte, isChildData bool, side BranchSide) ([]byte, error) {
	ctx := context.Background()

	meta := NewMetaNode()

	{
		metaReader, err := s.getter.Get(ctx, parent)
		if errors.Is(err, storage.ErrNotFound) {
			// ignore
		} else if err != nil {
			return nil, err
		} else {
			_, err = io.Copy(meta, metaReader)
			metaReader.Close()
			if err != nil {
				return nil, err
			}
		}
	}

	switch side {
	case LeftSide:
		copy(meta.left, child)
	case RightSide:
		copy(meta.right, child)
	}

	// err := s.remover.Remove(context.Background(), parent)
	// if errors.Is(err, storage.ErrNotFound) {
	// 	// ignore this situation, as parent might not be created yet
	// } else if err != nil {
	// 	return nil, err
	// }

	newParent, _, err := s.putter.Put(context.Background(), meta)
	if err != nil {
		return nil, err
	}

	return newParent, nil
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
