package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	branchNr  = 256
	entrySize = hashSize + 64 + 64 + 8
)

var ErrEntryNotFound = errors.New("no entry found")

type indexHeader struct {
	Magic   [4]byte
	Version uint32
	Created int64
	Opened  int64
}

type indexEntry struct {
	Hash   [hashSize]byte
	Offset int64
	Length int64
	Flags  int8
}

func newFileIndex(path string) (*fileIndex, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	index := &fileIndex{
		file:     f,
		branches: [branchNr][]*indexEntry{},
	}
	for i := range index.branches {
		index.branches[i] = make([]*indexEntry, 0, 32)
	}

	return index, index.load()
}

type fileIndex struct {
	file     *os.File
	branches [branchNr][]*indexEntry
}

func (f *fileIndex) load() error {
	header := indexHeader{}

	err := binary.Read(f.file, binary.LittleEndian, &header)
	if err != nil && err != io.EOF {
		return err
	}
	if err == io.EOF {
		header.Version = 1
		copy(header.Magic[:], "IDXO")
		header.Created = time.Now().Unix()
	}

	header.Opened = time.Now().Unix()

	if err := binary.Write(f.file, binary.LittleEndian, header); err != nil {
		return err
	}

	log.Infof("index create at :%d", header.Created)
	log.Infof("index opened at :%d", header.Opened)

	err = nil
	var entry indexEntry

	for err == nil {
		err = binary.Read(f.file, binary.LittleEndian, &entry)
		if err == nil {
			f.insertMem(entry.Hash[:], entry.Offset, entry.Length)
		}
	}

	dump(f)

	return nil
}

func (f *fileIndex) close() error {
	return f.file.Close()
}

func (f *fileIndex) insert(hash []byte, offset, length int64) error {
	entry, added, err := f.insertMem(hash, offset, length)
	if err != nil {
		return err
	}
	// in the case the already have the entry
	if !added {
		log.Debugln("entry already present")
		return nil
	}

	buf, err := encodeEntry(entry)
	if err != nil {
		return err
	}

	_, err = f.file.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (f *fileIndex) insertMem(hash []byte, offset, length int64) (*indexEntry, bool, error) {
	header, err := f.get(hash)
	if err != nil && err != ErrEntryNotFound {
		// actual error
		return nil, false, err

	}

	if header != nil {
		// entry already present
		return nil, false, nil
	}

	entry := &indexEntry{
		Offset: offset,
		Length: length,
	}
	copy(entry.Hash[:], hash)

	f.branches[hash[0]] = append(f.branches[hash[0]], entry)

	return entry, true, nil
}

func (f *fileIndex) get(hash []byte) (*dataHeader, error) {
	for i := range f.branches[hash[0]] {
		entry := f.branches[hash[0]][i]
		if bytes.Compare(entry.Hash[:], hash) == 0 {
			return &dataHeader{Offset: entry.Offset, Length: entry.Length}, nil
		}
	}

	return nil, ErrEntryNotFound
}

func dump(f *fileIndex) {
	for _, branch := range f.branches {
		for _, entry := range branch {
			fmt.Printf("hash:%x\n", string(entry.Hash[:]))
			fmt.Printf("offset:%d\n", entry.Offset)
			fmt.Printf("size:%d\n", entry.Length)
			fmt.Printf("---------\n")
		}
	}
}

func encodeEntry(entry *indexEntry) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
