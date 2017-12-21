package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	// dataSizeHeader is the size of the data header
	dataSizeHeader = 36
	// hashSize is the size of the hash used in the index
	hashSize = 32
)

// dataHeader is the data structure that prefixes all the data
// written on disk
type dataHeader struct {
	Offset int64
	Length int64
}

// newFileAppender creates a new FileAppender object
func newFileAppender(path string) (*fileAppender, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	_, err = f.Write([]byte("X"))

	return &fileAppender{
		file: f,
	}, err
}

// fileAppender implements the Appender interface
// using a file as backend
type fileAppender struct {
	file *os.File
}

// Close flush all the data on disk and
// properly close the underlining files
func (f *fileAppender) Close() error {
	return f.file.Close()
}

// Insert implements Appender.Insert interface
func (f *fileAppender) Insert(data []byte) ([]byte, *dataHeader, error) {
	var (
		header = &dataHeader{}
		err    error
	)

	header.Length = int64(len(data))
	header.Offset, err = f.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil, nil, err
	}

	hash := sha256.Sum256(data)

	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, hash); err != nil {
		return nil, nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(len(data))); err != nil {
		return nil, nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
		return nil, nil, err
	}

	if _, err := f.file.Write(buf.Bytes()); err != nil {
		return nil, nil, err
	}

	return hash[:], header, nil
}

// Get implements Appender.Get interface
func (f *fileAppender) Get(header *dataHeader) ([]byte, error) {
	buf := make([]byte, header.Length)
	n, err := f.file.ReadAt(buf, header.Offset+dataSizeHeader)
	if err != nil {
		return nil, err
	}

	if int64(n) != header.Length {
		return nil, fmt.Errorf("error during data read, wrong size")
	}

	return buf, nil
}
