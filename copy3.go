package main

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"syscall"
)

func onDirEntry(path string, f fs.FileInfo, err error) error {

	stat := f.Sys().(*syscall.Win32FileAttributeData)

	fmt.Printf(" %s/%s\n", path, f.Name())

	return nil
}

func main() {

	//filepath.WalkDir("/home/bee", onDirEntry)
	filepath.Walk("/home/bee", onDirEntry)
}
