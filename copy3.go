package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// stat := f.Sys().(*syscall.Win32FileAttributeData)
type ToCopy struct {
	path string
	info fs.FileInfo
}

func printErr(api string, err error, message string) {
	fmt.Printf("E: %s, %s, %s\n", api, err.Error(), message)
}

func createTargetfilename(sourceRootLen int, targetDir string, fullsourcename string) string {
	return filepath.Join(targetDir, fullsourcename[sourceRootLen+1:])
}

func createTargetDir(sourceRootLen int, targetDir string, sourceDirChan <-chan string) {
	for sourceDir := range sourceDirChan {
		var dir2create string

		if len(sourceDir) == sourceRootLen {
			dir2create = targetDir
		} else {
			dir2create = createTargetfilename(sourceRootLen, targetDir, sourceDir)
		}

		err := os.Mkdir(dir2create, os.ModeDir)
		if err != nil {
			if e, ok := err.(*os.PathError); ok {
				if errors.Is(e, fs.ErrExist) {
					continue
				} else {
					printErr("mkdir/PathError", err, dir2create)
				}
			} else {
				printErr("mkdir", err, dir2create)
			}
		}
	}
	fmt.Printf("I: createTargetDir ended (%s)\n", targetDir)
}

func enumerate(source string, targetDirs []string, files chan<- ToCopy) {

	defer close(files)

	var sourceDirChans []chan string

	for _, targetDir := range targetDirs {
		sourceDirChan := make(chan string)
		go createTargetDir(len(source), targetDir, sourceDirChan)
		sourceDirChans = append(sourceDirChans, sourceDirChan)
	}

	walkErr := filepath.Walk(source, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
		}

		if info.IsDir() {
			for _, c := range sourceDirChans {
				c <- path
			}
		} else {
			files <- ToCopy{path, info}
		}

		return nil
	})

	for _, c := range sourceDirChans {
		close(c)
	}

	if walkErr != nil {
		fmt.Println(walkErr)
	}

	fmt.Println("I: enumaration ended")
}

func startTargetWriters(source string, targetDirs []string, wg *sync.WaitGroup) ([]chan ToCopy, []chan []byte) {

	var targetFilenameChans []chan ToCopy
	var targetDataChans []chan []byte

	for _, targetDir := range targetDirs {
		fileinfo := make(chan ToCopy)
		datablocks := make(chan []byte)

		wg.Add(1)
		go createWriteTargetfile(len(source), targetDir, fileinfo, datablocks, wg)
		targetFilenameChans = append(targetFilenameChans, fileinfo)
		targetDataChans = append(targetDataChans, datablocks)
	}

	return targetFilenameChans, targetDataChans
}

func createWriteTargetfile(sourceRootLen int, targetDir string, fileinfos <-chan ToCopy, datablocks <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	for fileinfo := range fileinfos {
		targetfilename := createTargetfilename(sourceRootLen, targetDir, fileinfo.path)

		fp, err := os.Create(targetfilename)
		if err != nil {
			printErr("create file", err, targetfilename)
		} else {
			for {
				datablock := <-datablocks
				if len(datablock) == 0 { // EOF
					//fmt.Printf("copied %s\n", targetfilename)
					break
				} else {
					_, err := fp.Write(datablock)
					if err != nil {
						printErr("write file", err, targetfilename)
						break
					}
				}
			}
		}

		fp.Close()

		// TODO: set timestamps and attributes
	}

	fmt.Printf("targetWriter ended (%s)\n", targetDir)
}

func sendFileToTargets(fp *os.File, dataChans []chan []byte, bufs *[2][4096]byte) {
	bufIdx := 0
	for {
		bufIdx = 1 - bufIdx
		buf := bufs[bufIdx]
		numberRead, err := fp.Read(buf[:])
		if err != nil && !errors.Is(err, io.EOF) {
			printErr("read file", err, "")
		} else {
			for _, target := range dataChans {
				target <- buf[:numberRead] // send read bytes to all target writers
			}

			if numberRead == 0 {
				break
			}
		}
	}
}

func readHashCopy(source string, targetDirs []string, files <-chan ToCopy, wg *sync.WaitGroup) {
	defer wg.Done()
	targetFilenameChan, targetDataChan := startTargetWriters(source, targetDirs, wg)

	var bufs [2][4096]byte

	for file := range files {

		for _, target := range targetFilenameChan {
			target <- file // send filename to all target writers
		}

		fp, err := os.Open(file.path)
		if err != nil {
			printErr("open file", err, file.path)
		} else {
			sendFileToTargets(fp, targetDataChan, &bufs)
			fp.Close()
		}
	}

	fmt.Printf("readHashCopy ended\n")

	for _, c := range targetDataChan {
		close(c)
	}
	for _, c := range targetFilenameChan {
		close(c)
	}
}

func main() {

	source := "\\\\?\\c:\\tools"
	targets := []string{"\\\\?\\c:\\temp\\1", "\\\\?\\c:\\temp\\2"}

	const MAX_ENUMERATE = 100
	const COPY_WORKER = 1

	var wg sync.WaitGroup

	files := make(chan ToCopy, MAX_ENUMERATE)

	for i := 0; i < COPY_WORKER; i++ {
		wg.Add(1)
		go readHashCopy(source, targets, files, &wg)
	}

	go enumerate(source, targets, files)

	wg.Wait()
}
