package kvdb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/spf13/afero"
)

// No Keydir for now, just do a linear scan of the entire file

type DataStore struct {
	fs     afero.Fs
	path   string
	reader *record.Reader
	writer *record.Writer
}

// Create creates a single file datastore at the given path
func Create(fs afero.Fs, path string) (*DataStore, error) {
	if err := fs.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}

	// Write the file header
	err := datafile.WriteFileHeader(fs, path, datafile.NewFileHeader(time.Now(), 0))
	if err != nil {
		return nil, err
	}

	reader, err := record.NewReader(fs, path, datafile.FileHeaderSize)
	if err != nil {
		return nil, err
	}

	writer, err := record.NewWriter(fs, path)
	if err != nil {
		return nil, err
	}

	return &DataStore{
		fs:     fs,
		path:   path,
		reader: reader,
		writer: writer,
	}, nil
}

// Open opens the datastore at the specified location. If the datastore does not exist, an error is returned
func Open(fs afero.Fs, path string) (*DataStore, error) {
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotExist
	}
	// Check if it's a datafile
	header, err := datafile.ReadFileHeader(fs, path)
	if err != nil {
		return nil, err
	}

	reader, err := record.NewReader(fs, path, int64(header.Offset))
	if err != nil {
		return nil, err
	}

	writer, err := record.NewWriter(fs, path)
	if err != nil {
		return nil, err
	}

	return &DataStore{
		fs:     fs,
		path:   path,
		reader: reader,
		writer: writer,
	}, nil
}

// Get returns the value associated with the key. If the key does not exist, `ErrNotFound` is returned, in case of any
// other errors, the error is returned
func (dataStore *DataStore) Get(key []byte) ([]byte, error) {
	var offset int64 = 0
	var value []byte
	var valuePresent bool
	// TODO: It is more efficient to scan the datastore in reverse
	// Since we can stop at first match
	for {
		rec, err := dataStore.reader.ReadRecordAtStrict(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if string(rec.Key) == string(key) {
			if rec.Header.RecordType == record.RecordTypeDelete {
				valuePresent = false
				value = nil
			} else {
				value = rec.Value
				valuePresent = true
			}
		}
		offset += rec.Size
	}
	if valuePresent {
		return value, nil
	} else {
		return nil, ErrKeyNotFound
	}
}

// Put sets the value for the specified key. It returns an error if the operation was not successful
func (dataStore *DataStore) Put(key []byte, value []byte) error {
	_, err := dataStore.writer.WriteKeyValue(key, value)
	return err
}

// Delete deletes the value associated with the specified key. No error will be returned if the key does not exist.
// An error is returned if the deletion failed due to some other reason.
func (dataStore *DataStore) Delete(key []byte) error {
	_, err := dataStore.writer.WriteTombstone(key)
	return err
}

// ListKeys returns a list of all keys in the datastore. Note: This is intended to be
// used for debug or inspection.
func (dataStore *DataStore) ListKeys() ([][]byte, error) {
	keyMap := make(map[string]struct{})
	var offset int64 = 0
	for {
		rec, err := dataStore.reader.ReadKeyAt(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				keys := make([][]byte, 0, len(keyMap))
				for k := range keyMap {
					keys = append(keys, []byte(k))
				}
				return keys, nil
			}
			return nil, err
		}
		if rec.Header.RecordType == record.RecordTypeDelete {
			delete(keyMap, string(rec.Key))
		} else {
			keyMap[string(rec.Key)] = struct{}{}
		}
		offset += rec.Size
	}
}

// TODO: Implement fold

func (dataStore *DataStore) Merge(directoryPath string) error {
	// nop
	return nil
}

func (dataStore *DataStore) Sync() error {
	return dataStore.writer.Sync()
}

// Size returns the number of keys present in the datastore
func (dataStore *DataStore) Size() int {
	// TODO: Implement later
	return 0
}

// Close closes the datastore, writes pending changes (if any), and frees resources
func (dataStore *DataStore) Close() error {
	if err := dataStore.writer.Sync(); err != nil {
		return err
	}
	err1 := dataStore.writer.Close()
	err2 := dataStore.reader.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}
