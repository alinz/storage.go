package merkle_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go/hash"
	"github.com/alinz/storage.go/internal/tests"
	"github.com/alinz/storage.go/local"
	"github.com/alinz/storage.go/merkle"
)

type TestNode struct {
	id       string
	fileType merkle.FileType
	left     string
	right    string
	value    []byte
}

func TestMerkleStoragePut(t *testing.T) {
	tempDir := t.TempDir()

	prepareTempDir := func(t *testing.T, i int) string {
		testCaseFolder := path.Join(tempDir, fmt.Sprintf("%d", i))
		err := os.Mkdir(testCaseFolder, os.ModePerm)
		assert.NoError(t, err)
		return testCaseFolder
	}

	testCases := []struct {
		// configuration
		blockSize int64
		content   []byte

		// expectations
		writtenBytes int64
		nodes        []TestNode
	}{
		{
			blockSize: 1,
			content:   []byte{1},

			writtenBytes: 1,

			nodes: []TestNode{
				{
					id:       "sha256-2498ad992b02c2f6e21684e8057a01463acad5c75a4e75d095619c556a559e8c",
					fileType: merkle.MetaType,
					left:     "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					right:    "sha256-0000000000000000000000000000000000000000000000000000000000000000",
				},
				{
					id:       "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					fileType: merkle.DataType,
					value:    []byte{1},
				},
			},
		},
		{
			blockSize: 1,
			content:   []byte{1, 1},

			writtenBytes: 2,

			nodes: []TestNode{
				{
					id:       "sha256-a2e8f8c5d9f23620c8c4231988eb74ca6f7fa940454b8cbb19d2c2c1333d8316",
					fileType: merkle.MetaType,
					left:     "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					right:    "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
				},
				{
					id:       "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					fileType: merkle.DataType,
					value:    []byte{1},
				},
			},
		},
		{
			blockSize: 1,
			content:   []byte{1, 1, 1},

			writtenBytes: 3,

			nodes: []TestNode{
				// ROOT
				{
					id:       "sha256-f16285f5de972f7414a12523dc870fd6cfc34fd0a6a0764869d7958d4a296278",
					fileType: merkle.MetaType,
					left:     "sha256-a2e8f8c5d9f23620c8c4231988eb74ca6f7fa940454b8cbb19d2c2c1333d8316",
					right:    "sha256-2498ad992b02c2f6e21684e8057a01463acad5c75a4e75d095619c556a559e8c",
				},
				// ROOT -> LEFT
				{
					id:       "sha256-a2e8f8c5d9f23620c8c4231988eb74ca6f7fa940454b8cbb19d2c2c1333d8316",
					fileType: merkle.MetaType,
					left:     "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					right:    "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
				},
				// ROOT -> RIGHT
				{
					id:       "sha256-2498ad992b02c2f6e21684e8057a01463acad5c75a4e75d095619c556a559e8c",
					fileType: merkle.MetaType,
					left:     "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					right:    "sha256-0000000000000000000000000000000000000000000000000000000000000000",
				},
				{
					id:       "sha256-25dfd29c09617dcc9852281c030e5b3037a338a4712a42a21c907f259c6412a0",
					fileType: merkle.DataType,
					value:    []byte{1},
				},
			},
		},
	}

	ctx := context.Background()

	for i, testCase := range testCases {

		path := prepareTempDir(t, i)

		fmt.Printf("Path for test %d: %s\n", i+1, path)

		localStorage := local.New(path)
		merkleStorage := merkle.New(localStorage, localStorage, testCase.blockSize)

		_, n, err := merkleStorage.Put(context.TODO(), bytes.NewReader(testCase.content))
		assert.NoError(t, err)
		assert.Equal(t, testCase.writtenBytes, n)

		for _, node := range testCase.nodes {
			currentHash, err := hash.ParseValueFromString(node.id)
			assert.NoError(t, err)

			func() {
				assert.Equal(t, node.id, hash.Format(currentHash))
				rc, err := localStorage.Get(ctx, currentHash)
				assert.NoError(t, err)
				defer rc.Close()
				r, fileType, err := merkle.DetectFileType(rc)
				assert.NoError(t, err)
				assert.Equal(t, node.fileType, fileType)

				switch fileType {
				case merkle.DataType:
					data, err := merkle.ParseDataFile(r)
					assert.NoError(t, err)
					assert.NoError(t, tests.EqualReaders(data, bytes.NewReader(node.value)))
				case merkle.MetaType:
					meta, err := merkle.ParseMetaFile(r)
					assert.NoError(t, err)
					assert.Equal(t, node.left, hash.Format(meta.Left()))
					assert.Equal(t, node.right, hash.Format(meta.Right()))
				}
			}()

		}
	}
}

func TestMerkleStorage(t *testing.T) {
	tempDir := t.TempDir()
	localStorage := local.New(tempDir)
	merkleStorage := merkle.New(localStorage, localStorage, 1)

	fmt.Println(tempDir)

	data := []byte{1, 1, 1}
	dataReader := bytes.NewReader(data)

	hash, n, err := merkleStorage.Put(context.Background(), dataReader)
	assert.NoError(t, err)
	assert.Equal(t, len(data), int(n))
	assert.NotNil(t, hash)

	r, err := merkleStorage.Get(context.Background(), hash)
	assert.NoError(t, err)
	err = tests.EqualReaders(bytes.NewReader(data), r)
	assert.NoError(t, err)
}
