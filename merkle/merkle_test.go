package merkle_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/local"
	"github.com/alinz/storage.go/merkle"
	"github.com/stretchr/testify/assert"
)

func TestMerklePutter(t *testing.T) {
	testCases := []struct {
		content    string
		blockSize  int64
		filesCount int
	}{
		// {
		// 	content:    "123456789",
		// 	blockSize:  9,
		// 	filesCount: 2,
		// },
		// {
		// 	content:    "123456789123456789",
		// 	blockSize:  9,
		// 	filesCount: 3,
		// },
		// {
		// 	content:    "123456789123456789123456789",
		// 	blockSize:  9,
		// 	filesCount: 4,
		// },
		// {
		// 	content:    "123456789123456789123456789123456789",
		// 	blockSize:  9,
		// 	filesCount: 5,
		// },
		// {
		// 	content:    "123456789123456789123456789123456789123456789",
		// 	blockSize:  9,
		// 	filesCount: 6,
		// },
		// {
		// 	content:    "hello world, this is a great time to be alive.",
		// 	blockSize:  9,
		// 	filesCount: 18,
		// },
		// {
		// 	content:    "hello world, nice to ",
		// 	blockSize:  10,
		// 	filesCount: 8,
		// },
		{
			content:    "123",
			blockSize:  1,
			filesCount: 8,
		},
	}

	for _, testCase := range testCases {
		tempPath := t.TempDir()
		local := local.New(tempPath)
		storage := merkle.New(local, local, local, testCase.blockSize)

		hashValue, n, err := storage.Put(context.Background(), strings.NewReader(testCase.content))
		assert.NoError(t, err)
		assert.NotNil(t, hashValue)
		assert.Equal(t, int64(len(testCase.content)), n)
		assert.Equal(t, testCase.filesCount, tests.CountFiles(t, tempPath))
	}
}

func TestMerkleGetter(t *testing.T) {
	testCases := []struct {
		content   string
		blockSize int64
	}{
		{
			content:   "hello world, nice to ",
			blockSize: 5,
		},
	}

	for _, testCase := range testCases {
		func() {
			tempPath := t.TempDir()

			fmt.Println(tempPath)

			local := local.New(tempPath)
			storage := merkle.New(local, local, local, testCase.blockSize)

			hashValue, _, _ := storage.Put(context.Background(), strings.NewReader(testCase.content))

			// hash.Print(hashValue, os.Stdout)

			r, err := storage.Get(context.Background(), hashValue)
			assert.NoError(t, err)
			defer r.Close()

			// b, _ := ioutil.ReadAll(r)
			// fmt.Println("#########", string(b), "==", testCase.content)

			assert.NoError(t, tests.EqualReaders(r, strings.NewReader(testCase.content)))
		}()
	}
}
