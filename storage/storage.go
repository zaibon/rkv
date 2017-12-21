package storage

import (
	"fmt"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

// Storage is the struct responsible to
// write and read data to/from disk
type Storage struct {
	data  *fileAppender
	index *fileIndex
}

// New creates a new storage object that will use file inside the
// directory pointed by dir
func New(dir string) (*Storage, error) {
	data, err := newFileAppender(filepath.Join(dir, "rkv-data"))
	if err != nil {
		return nil, err
	}

	index, err := newFileIndex(filepath.Join(dir, "rkv-index"))
	if err != nil {
		return nil, err
	}
	return &Storage{
		data:  data,
		index: index,
	}, nil
}

// Set write data onto the disk and return the hash of the data and
// and error is any
func (s *Storage) Set(data []byte) ([]byte, error) {
	log.Debugln("trying to insert entry")
	hash, header, err := s.data.Insert(data)
	if err != nil {
		log.Errorf("error inserting data: %v", err)
		return nil, err
	}

	err = s.index.insert(hash, header.Offset, header.Length)
	return hash, err
}

// Get read data identified by hash from the disk and return the
// data and an error is any
func (s *Storage) Get(hash []byte) ([]byte, error) {
	index, err := s.index.get(hash)
	if err != nil {
		return nil, err
	}

	return s.data.Get(&dataHeader{
		Offset: index.Offset,
		Length: index.Length,
	})
}

func (s *Storage) Close() error {
	fmt.Println("stop")
	fmt.Println("close data")
	if err := s.data.Close(); err != nil {
		return err
	}
	fmt.Println("close index")
	if err := s.index.close(); err != nil {
		return err
	}
	return nil
}
