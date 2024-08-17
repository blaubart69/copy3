package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// stat := f.Sys().(*syscall.Win32FileAttributeData)
type ToCopy struct {
	path string
	info fs.FileInfo
}

type Stats struct {
	filesRead    uint64
	filesWritten uint64
	bytesRead    uint64
	bytesWritten uint64
	dirsCreated  uint64
}

var stats = Stats{}

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
		} else {
			atomic.AddUint64(&stats.dirsCreated, 1)
		}
	}
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
}

func startTargetWriters(source string, targetDirs []string, wg *sync.WaitGroup) ([]chan ToCopy, []chan []byte) {

	var targetFilenameChans []chan ToCopy
	var targetDataChans []chan []byte

	for _, targetDir := range targetDirs {
		fileinfo := make(chan ToCopy)
		datablocks := make(chan []byte)

		wg.Add(1)
		go createWriteTargetfiles(len(source), targetDir, fileinfo, datablocks, wg)
		targetFilenameChans = append(targetFilenameChans, fileinfo)
		targetDataChans = append(targetDataChans, datablocks)
	}

	return targetFilenameChans, targetDataChans
}

func createWriteTargetfiles(sourceRootLen int, targetDir string, fileinfos <-chan ToCopy, datablocks <-chan []byte, wg *sync.WaitGroup) {
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
					atomic.AddUint64(&stats.filesWritten, 1)
					break
				} else {
					written, err := fp.Write(datablock)
					if err != nil {
						printErr("write file", err, targetfilename)
						break
					} else {
						atomic.AddUint64(&stats.bytesWritten, uint64(written))
					}
				}
			}
		}
		stat := fileinfo.info.Sys().(*syscall.Win32FileAttributeData)

		setTimeErr := syscall.SetFileTime(
			syscall.Handle(fp.Fd()),
			&stat.CreationTime,
			&stat.LastAccessTime,
			&stat.LastWriteTime)

		if setTimeErr != nil {
			printErr("SetFileTime", setTimeErr, targetfilename)
		}

		fp.Close()

		// TODO: set timestamps and attributes
	}
}

// read a source file block by block.
// send byte slice to all writers.
// last slice has a len of zero. indicating EOF.
func copyFileToTargets(filename string, dataChans []chan []byte, bufs *[2][4096]byte) {
	fp, err := os.Open(filename)
	if err != nil {
		printErr("open file", err, filename)
	} else {
		defer fp.Close()
		bufIdx := 0
		for {
			bufIdx = 1 - bufIdx
			buf := bufs[bufIdx]
			numberRead, err := fp.Read(buf[:])
			if err != nil && !errors.Is(err, io.EOF) {
				printErr("read file", err, "")
			} else {
				atomic.AddUint64(&stats.bytesRead, uint64(numberRead))

				for _, target := range dataChans {
					target <- buf[:numberRead] // send read bytes to all target writers
				}

				if numberRead == 0 {
					break
				}
			}
		}
		atomic.AddUint64(&stats.filesRead, 1)
	}
}

func readHashCopy(source string, targetDirs []string, files <-chan ToCopy, wg *sync.WaitGroup) {
	defer wg.Done()
	targetFilenameChans, targetDataChans := startTargetWriters(source, targetDirs, wg)

	var bufs [2][4096]byte

	for file := range files {

		for _, target := range targetFilenameChans {
			target <- file // send filename to all target writers
		}

		copyFileToTargets(file.path, targetDataChans, &bufs)
	}

	for _, c := range targetDataChans {
		close(c)
	}
	for _, c := range targetFilenameChans {
		close(c)
	}
}

func printStats(stats *Stats) {
	fmt.Printf("read: %d/%d\twrite: %d/%d\tdirs created %d\n",
		atomic.LoadUint64(&stats.filesRead),
		atomic.LoadUint64(&stats.bytesRead),
		atomic.LoadUint64(&stats.filesWritten),
		atomic.LoadUint64(&stats.bytesWritten),
		atomic.LoadUint64(&stats.dirsCreated))
}

func main() {

	source := "\\\\?\\c:\\tools"
	targets := []string{"\\\\?\\c:\\temp\\1", "\\\\?\\c:\\temp\\2"}

	const MAX_ENUMERATE = 100
	const READ_WORKER = 16

	var wg sync.WaitGroup

	files := make(chan ToCopy, MAX_ENUMERATE)

	for i := 0; i < READ_WORKER; i++ {
		wg.Add(1)
		go readHashCopy(source, targets, files, &wg)
	}

	go enumerate(source, targets, files)

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

loop:
	for {
		select {
		case <-finished:
			printStats(&stats)
			fmt.Println("done")
			break loop
		case <-time.After(1 * time.Second):
			printStats(&stats)
		}
	}
}
