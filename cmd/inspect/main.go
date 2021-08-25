package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/alinz/storage.go/hash"
	"github.com/alinz/storage.go/merkle"
)

func main() {
	var path string

	flag.StringVar(&path, "path", "", "path to any merkle's storage file")

	flag.Parse()

	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fmt.Println("File name: ", file.Name())
		showInfo(filepath.Join(path, file.Name()))
		fmt.Println("#####")
		fmt.Println("")
	}
}

func showInfo(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	r, fileType, err := merkle.DetectFileType(file)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("File Type: ", fileType.String())

	switch fileType {
	case merkle.DataType:

	case merkle.MetaType:
		meta, err := merkle.ParseMetaFile(r)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("LEFT: ", hash.Format(meta.Left()))
		fmt.Println("RIGHT: ", hash.Format(meta.Right()))
	}
}
