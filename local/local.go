package local

import (
	"context"
	"crypto/rand"
	"io"
	"math/big"
	"os"
	"path/filepath"

	"github.com/alinz/hash.go"

	"github.com/alinz/storage.go"
)

type Storage struct {
	path string
}

var _ storage.Putter = (*Storage)(nil)
var _ storage.Getter = (*Storage)(nil)
var _ storage.Remover = (*Storage)(nil)

func (s *Storage) Put(ctx context.Context, r io.Reader) ([]byte, int64, error) {
	// generate random filename
	tempFilename, err := generateRandomString(10)
	if err != nil {
		return nil, 0, err
	}

	// create the file name in given path
	tempFilePath := filepath.Join(s.path, tempFilename)
	file, err := os.Create(tempFilePath)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	// need to do couple of things here,
	// 1: write the given io.Reader to file
	// 2: calculate the hash value
	// 3: calculate the size of the written file
	cr := hash.NewReader(r)
	n, err := io.Copy(file, cr)
	if err != nil {
		return nil, 0, err
	} else if n == 0 {
		os.Remove(tempFilePath)
		return nil, 0, io.EOF
	}

	hash := cr.Hash()
	filePath := filepath.Join(s.path, hash.String())

	// if filePath is already exists, no need to rename the file
	// we just have to remove the temporary file
	_, err = os.Stat(filePath)
	if os.IsExist(err) {
		os.Remove(tempFilePath)
		return hash, n, nil
	} else if os.IsNotExist(err) {
		// ignore this as we are about to create a new file
	} else if err != nil {
		return nil, n, err
	}

	err = os.Rename(tempFilePath, filePath)
	if err != nil {
		return nil, 0, err
	}

	return hash, n, nil
}

func (s *Storage) Get(ctx context.Context, hashValue []byte) (io.ReadCloser, error) {
	internalHash := hash.Value(hashValue)
	filePath := filepath.Join(s.path, internalHash.String())

	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return nil, storage.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (s *Storage) Remove(ctx context.Context, hashValue []byte) error {
	internalHash := hash.Value(hashValue)
	filePath := filepath.Join(s.path, internalHash.String())

	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return storage.ErrNotFound
	} else if err != nil {
		return err
	}

	return os.Remove(filePath)
}

func New(path string) *Storage {
	return &Storage{
		path: path,
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func generateRandomString(n int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret), nil
}
