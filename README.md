# Storage.go

This is an abstraction on top of exisiting filesystem to provide tamperproff feature using merkle-tree.

## Features

- Provides a simple one method interfaces

```go
type Putter interface {
	Put(ctx context.Context, r io.Reader) ([]byte, int64, error)
}

type Getter interface {
	Get(ctx context.Context, hash []byte) (io.ReadCloser, error)
}

type Remover interface {
	Remove(ctx context.Context, hash []byte) error
}

type Lister interface {
	List() (IteratorFunc, CancelFunc)
}

type Closer interface {
	Close(ctx context.Context, hash []byte) error
}
```

- Optimized merkle tree for fast write
- Support io.Reader out of the box
- Dedup files by default using SHA-256 hash
- Secure Read and Write using ChaCha20Stream
- Lots of backend drivers (memory, file, boltdb, pogreb, sqlite)


## Example

let's build a simple command line tools to put content into merkle storage and retrive them using their root hash

```go
package main

import (
	"context"
	"io"
	"os"

	"github.com/alinz/hash.go"

	"github.com/alinz/storage.go/local"
	"github.com/alinz/storage.go/merkle"
)

const BlockSize = 10
const StoragePath = "./merkle_storage"

func main() {
	os.MkdirAll(StoragePath, os.ModePerm)

	local := local.New(StoragePath)
	merkle := merkle.New(local, local, BlockSize)

	switch os.Args[1] {
	case "put":
		value, n, err := merkle.Put(context.Background(), os.Stdin)
		if err != nil {
			println(err)
			os.Exit(1)
		}

		println("size: ", n)
		println("key", hash.Format(value))

	case "get":
		key := os.Args[2]
		value, err := hash.ValueFromString(key)
		if err != nil {
			println(err)
			os.Exit(1)
		}

		r, err := merkle.Get(context.Background(), value)
		if err != nil {
			println(err)
			os.Exit(1)
		}
		defer r.Close()
		io.Copy(os.Stdout, r)

	default:
		os.Exit(1)
	}
}
```

now run the following command to put the content in to merkle storage

```bash
echo "hello world" | go run main.go put

size:  12
key sha256-cc86f92f36f12d9aeb48391a3a47ad559110eae2b770a3b7650cb7fb8854f07f
```

and run the following to retrive it

```bash
go run main.go get sha256-cc86f92f36f12d9aeb48391a3a47ad559110eae2b770a3b7650cb7fb8854f07f
```
