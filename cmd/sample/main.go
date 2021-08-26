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
