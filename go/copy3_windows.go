package main

import (
	"crypto/sha256"
	"encoding/hex"
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

func printStats(stats *Stats) {
	fmt.Printf("read: %d/%d\twrite: %d/%d\tdirs created %d\n",
		atomic.LoadUint64(&stats.filesRead),
		atomic.LoadUint64(&stats.bytesRead),
		atomic.LoadUint64(&stats.filesWritten),
		atomic.LoadUint64(&stats.bytesWritten),
		atomic.LoadUint64(&stats.dirsCreated))
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

func createFileForWriting(targetfilename string, fileAttributes uint32) (*os.File, error) {
	if strptr, err := syscall.UTF16PtrFromString(targetfilename); err != nil {
		return nil, err
	} else if handle, err := syscall.CreateFile(
		strptr,
		syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ,
		nil,
		syscall.CREATE_ALWAYS,
		fileAttributes,
		0); err != nil {
		return nil, err
	} else {
		fp := os.NewFile(uintptr(handle), targetfilename)
		return fp, nil
	}
}

func createWriteTargetfiles(sourceRootLen int, targetDir string, fileinfos <-chan ToCopy, datablocks <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	for fileinfo := range fileinfos {
		targetfilename := createTargetfilename(sourceRootLen, targetDir, fileinfo.path)
		stat := fileinfo.info.Sys().(*syscall.Win32FileAttributeData)

		fp, err := createFileForWriting(targetfilename, stat.FileAttributes)
		if err != nil {
			fmt.Printf("E: create target file. %s (skip file) %s", err, targetfilename)
		}

		for {
			datablock := <-datablocks
			if len(datablock) == 0 { // EOF
				//fmt.Printf("copied %s\n", targetfilename)
				if fp != nil {
					atomic.AddUint64(&stats.filesWritten, 1)
				}
				break
			} else if fp == nil {
				// skip the data but consume it
			} else if written, err := fp.Write(datablock); err != nil {
				printErr("write file", err, targetfilename)
				break
			} else {
				atomic.AddUint64(&stats.bytesWritten, uint64(written))
			}
		}
		if fp != nil {
			setTimeErr := syscall.SetFileTime(
				syscall.Handle(fp.Fd()),
				&stat.CreationTime,
				&stat.LastAccessTime,
				&stat.LastWriteTime)

			if setTimeErr != nil {
				printErr("SetFileTime", setTimeErr, targetfilename)
			}

			fp.Close()
		}
	}
}

// read a source file block by block.
// send byte slice to all writers.
// last slice has a len of zero. indicating EOF.
func readFileSendToTargets(filename string, dataChans []chan []byte, bufs *[2][4096]byte) {
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

type HashResult struct {
	filename string
	hash     []byte
}

func hasher(filenames <-chan string, filedata <-chan []byte, hashes chan<- HashResult) {

	h := sha256.New()

	for filename := range filenames {
		for data := range filedata {
			if len(data) == 0 {
				hashes <- HashResult{filename, h.Sum(nil)}
				h.Reset()
				break
			} else {
				h.Write(data)
			}
		}
	}
}

func hashWriter(filename string, hashes <-chan HashResult) {
	fp, err := os.Create(filename)
	if err != nil {
		panic(fmt.Sprintf("could not create result file for hashes. err: %s", err))
	} else {
		defer fp.Close()

		for hash := range hashes {
			fp.WriteString(fmt.Sprintf("%s %s\n", hex.EncodeToString(hash.hash), hash.filename))
		}
	}
}

func readFiles(source string, targetDirs []string, files <-chan ToCopy, hashes chan<- HashResult, wg *sync.WaitGroup) {

	defer wg.Done()
	sourceLen := len(source)
	targetFilenameChans, targetDataChans := startTargetWriters(source, targetDirs, wg)
	hasherFilenames := make(chan string)
	hasherData := make(chan []byte)

	// send data blocks of the files also to the hasher
	targetDataChans = append(targetDataChans, hasherData)

	go hasher(hasherFilenames, hasherData, hashes)

	var bufs [2][4096]byte
	for file := range files {

		for _, target := range targetFilenameChans {
			target <- file // send file to all target writers
		}
		relativeFilename := file.path[sourceLen+1:]
		hasherFilenames <- relativeFilename

		readFileSendToTargets(file.path, targetDataChans, &bufs)
	}

	for _, c := range targetDataChans {
		close(c)
	}
	for _, c := range targetFilenameChans {
		close(c)
	}
	close(hasherFilenames)
}

func main() {

	source := "\\\\?\\c:\\tools"
	targets := []string{"\\\\?\\c:\\temp\\1", "\\\\?\\c:\\temp\\2"}

	const MAX_ENUMERATE = 100
	const READ_WORKER = 16

	var wg sync.WaitGroup

	// channel from enumerate to read files
	files := make(chan ToCopy, MAX_ENUMERATE)

	// channel to the writer of hashes
	hashes := make(chan HashResult)
	go hashWriter("./hashes.txt", hashes)

	// go routines that read the content of the incoming files
	// 	1, send filenames and contents to a "create and write" routine
	//  2, send filenames and contents to a hasher routine
	for i := 0; i < READ_WORKER; i++ {
		wg.Add(1)
		go readFiles(source, targets, files, hashes, &wg)
	}

	go enumerate(source, targets, files)

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(hashes)
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
