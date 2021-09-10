package merkle_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/alinz/hash.go"
	"github.com/stretchr/testify/assert"

	"github.com/alinz/storage.go"
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
					id:       "sha256-c3309091158720caf364bcb3681830924b4168c663db924c00eb401a20438205",
					fileType: merkle.RootType,
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
					id:       "sha256-65276806945c01f329540212084b45ced8f86de1bd8c13410034a2bcd887bf42",
					fileType: merkle.RootType,
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
					id:       "sha256-a05e479475e7161b650e618ac3fc16c9b99c7dbdb96e2b41ab31d5490c76e7d6",
					fileType: merkle.RootType,
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
		{
			blockSize: 10,
			content:   []byte("hello world"),

			writtenBytes: 11,

			nodes: []TestNode{
				// ROOT
				{
					id:       "sha256-99dc22058051e167aec7d2930ec098c8d5b09037ab56a8ce3ebdbdedee0f2175",
					fileType: merkle.RootType,
					left:     "sha256-fe7a3cfc8c5e2ce3334d6ede26904a9fc9f077c685883fe59f782d5cf7239450",
					right:    "sha256-fa345019a25f632945e06308a3369199bffbed38ae888d91378857677bc544cd",
				},
				// ROOT -> LEFT -> LEFT
				{
					id:       "sha256-fe7a3cfc8c5e2ce3334d6ede26904a9fc9f077c685883fe59f782d5cf7239450",
					fileType: merkle.DataType,
					value:    []byte("hello worl"),
				},
				// ROOT -> LEFT -> RIGHT
				{
					id:       "sha256-fa345019a25f632945e06308a3369199bffbed38ae888d91378857677bc544cd",
					fileType: merkle.DataType,
					value:    []byte("d"),
				},
			},
		},
	}

	ctx := context.Background()

	for i, testCase := range testCases {

		path := prepareTempDir(t, i)

		fmt.Printf("Path for test %d: %s\n", i+1, path)

		localStorage := local.New(path)
		merkleStorage := merkle.New(localStorage, localStorage, localStorage, testCase.blockSize)

		_, n, err := merkleStorage.Put(context.TODO(), bytes.NewReader(testCase.content))
		assert.NoError(t, err)
		assert.Equal(t, testCase.writtenBytes, n)

		for _, node := range testCase.nodes {
			currentHash, err := hash.ValueFromString(node.id)
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

func TestMerkleTreeList(t *testing.T) {

	testCases := []struct {
		contents [][]byte
		roots    map[string]interface{}
	}{
		{
			contents: [][]byte{
				[]byte("hello world"),
				[]byte("hello world 12345"),
				[]byte("this is one of the kind"),
			},

			roots: map[string]interface{}{
				"sha256-99dc22058051e167aec7d2930ec098c8d5b09037ab56a8ce3ebdbdedee0f2175": nil,
				"sha256-275ecb532fa776f9fc859043b2238a9a094610bc9cb59761e19e3a1c05aecd4f": nil,
				"sha256-b9dff0f92c0c415b51a04bcb923bc87667c7e9dc8bff4022df1865cd1020080c": nil,
			},
		},
	}

	for _, testCase := range testCases {
		tempDir := t.TempDir()
		localStorage := local.New(tempDir)
		merkleStorage := merkle.New(localStorage, localStorage, localStorage, 10)

		hashValues := make(map[string]interface{})

		for _, content := range testCase.contents {
			hashValue, _, err := merkleStorage.Put(context.Background(), bytes.NewReader(content))
			assert.NoError(t, err)

			hashValues[hash.Format(hashValue)] = nil
		}

		assert.Equal(t, testCase.roots, hashValues)

		roots := make(map[string]interface{})

		next, cancel := merkleStorage.List()
		defer cancel()
		for {
			hashValue, err := next(context.Background())

			if errors.Is(err, storage.ErrIteratorDone) {
				break
			} else if err != nil {
				t.Fatal(err)
			}

			roots[hash.Format(hashValue)] = nil
		}

		assert.Equal(t, testCase.roots, roots)
	}
}

func TestMerkleStorage(t *testing.T) {
	tempDir := t.TempDir()
	localStorage := local.New(tempDir)
	merkleStorage := merkle.New(localStorage, localStorage, localStorage, 1)

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
