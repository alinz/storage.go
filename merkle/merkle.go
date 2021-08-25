package merkle

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/alinz/storage.go"
)

type Storage struct {
	BlockSize int64
	Putter    storage.Putter
	Getter    storage.Getter
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	var totalSize int64
	var totalBlocks int64
	var actualSize int64

	tree := NewTree(s.rebalance)

	for {
		dataFile := NewDataFile(io.LimitReader(r, s.BlockSize))
		hashValue, n, err := s.Putter.Put(ctx, dataFile)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, actualSize, err
		} else if n == 0 {
			break
		}

		totalBlocks++
		totalSize += n
		actualSize = totalSize - totalBlocks

		err = tree.Add(hashValue)
		if err != nil {
			return nil, actualSize, err
		}
	}

	return tree.root().Value, actualSize, nil
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	stack := NewBytesStack()
	stack.Push(hashValue)

	walk := func() error {
		value := stack.Pop()

		r, err := s.Getter.Get(ctx, value)
		if err != nil {
			return err
		}
		defer r.Close()

		reader, fileType, err := DetectFileType(r)
		if err != nil {
			return err
		}

		if fileType == MetaType {
			metaFile, err := ParseMetaFile(reader)
			if err != nil {
				return err
			}

			if metaFile.HasRight() {
				stack.Push(metaFile.right)
			}

			if metaFile.HasLeft() {
				stack.Push(metaFile.left)
			}

		} else if fileType == DataType {
			reader, _ = ParseDataFile(reader)
			_, err = io.Copy(pw, reader)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("data corruption")
		}

		return nil
	}

	go func() {
		var err error

		for !stack.IsEmpty() {
			err = walk()
			if err != nil {
				break
			}
		}

		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	return pr, nil
}

func (s *Storage) rebalance(parent []byte, child []byte, side NodeSide) ([]byte, error) {
	ctx := context.Background()

	metaFile, err := s.readMetaFile(ctx, parent)
	if err != nil {
		return nil, err
	}

	switch side {
	case LeftSide:
		copy(metaFile.left, child)
	case RightSide:
		copy(metaFile.right, child)
	}

	newValue, _, err := s.Putter.Put(ctx, metaFile)
	if err != nil {
		return nil, err
	}

	return newValue, nil
}

func (s *Storage) readMetaFile(ctx context.Context, key []byte) (*MetaFile, error) {
	metaFile := NewMetaFile()

	metaFileReader, err := s.Getter.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		// ignore
		err = nil
	} else if err != nil {
		return nil, err
	} else {
		_, err = io.Copy(metaFile, metaFileReader)
		metaFileReader.Close()
	}

	return metaFile, err
}
