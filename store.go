package kvdb

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/filemanager"
	"github.com/ananthvk/kvdb/internal/keydir"
	"github.com/ananthvk/kvdb/internal/metafile"
	"github.com/spf13/afero"
)

// No Keydir for now, just do a linear scan of the entire file

type DataStore struct {
	fs          afero.Fs
	path        string
	metaInfo    *metafile.MetaData
	keydir      *keydir.Keydir
	fileManager *filemanager.FileManager
}

const (
	datastoreType          = "kvdb"  // Type of store
	version                = "1.0.0" // Version of the application
	defaultMaxKeySize      = 128     // In bytes (128 bytes)
	defaultMaxValueSize    = 64000   // In bytes (64 KB)
	defaultMaxDatafileSize = 50      // In bytes (50 bytes for testing)
)

// Create creates a datastore at the given path, if the path exists and an existing key store
// is found, it returns an error. If the path is a file, or is a non empty directory, an error
// is returned. Otherwise, the directory is created (along with all it's parents), and the datastore
// is initialized
func Create(fs afero.Fs, path string) (*DataStore, error) {
	// Check if it's a valid path to create a datastore
	if valid, reason, err := metafile.IsValidPath(fs, path); err != nil || !valid {
		if err != nil {
			return nil, err
		} else {
			return nil, errors.New(reason)
		}
	}

	if err := fs.MkdirAll((path), os.ModePerm); err != nil {
		return nil, err
	}

	metainfo := &metafile.MetaData{
		Type:            datastoreType,
		Version:         version,
		Created:         time.Now().String(),
		MaxKeySize:      defaultMaxKeySize,
		MaxValueSize:    defaultMaxValueSize,
		MaxDatafileSize: defaultMaxDatafileSize,
	}
	// Write the metafile
	if err := metafile.WriteMetaFile(fs, path, metainfo); err != nil {
		return nil, err
	}

	// Make the data/ folder
	if err := fs.Mkdir(filepath.Join(path, "data"), os.ModePerm); err != nil {
		return nil, err
	}
	fm, err := filemanager.NewFileManager(fs, path, defaultMaxDatafileSize)
	if err != nil {
		return nil, err
	}
	return &DataStore{
		fs:          fs,
		path:        path,
		metaInfo:    metainfo,
		keydir:      keydir.NewKeydir(),
		fileManager: fm,
	}, nil
}

// Open opens the datastore at the specified location. If the datastore does not exist, an error is returned
func Open(fs afero.Fs, path string) (*DataStore, error) {
	exists, err := metafile.IsDatastore(fs, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotExist
	}

	// Read the metafile
	metainfo, err := metafile.ReadMetaFile(fs, path)
	if err != nil {
		return nil, err
	}
	if metainfo.Type != "kvdb" {
		return nil, errors.New("metafile corrupted, not a kvdb")
	}

	fm, err := filemanager.NewFileManager(fs, path, defaultMaxDatafileSize)
	if err != nil {
		return nil, err
	}
	kd, err := fm.ReadKeydir()
	if err != nil {
		return nil, err
	}
	return &DataStore{
		fs:          fs,
		path:        path,
		keydir:      kd,
		metaInfo:    metainfo,
		fileManager: fm,
	}, nil
}

// Get returns the value associated with the key. If the key does not exist, `ErrNotFound` is returned, in case of any
// other errors, the error is returned
func (dataStore *DataStore) Get(key []byte) ([]byte, error) {
	rec, ok := dataStore.keydir.GetKeydirRecord(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	record, err := dataStore.fileManager.ReadValueAt(rec.FileId, rec.ValuePos)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

// Put sets the value for the specified key. It returns an error if the operation was not successful
func (dataStore *DataStore) Put(key []byte, value []byte) error {
	fileId, offset, err := dataStore.fileManager.Write(key, value, false)
	dataStore.keydir.AddKeydirRecord(key, fileId, uint32(len(value)), offset-datafile.FileHeaderSize, time.Now())
	return err
}

// Delete deletes the value associated with the specified key. No error will be returned if the key does not exist.
// An error is returned if the deletion failed due to some other reason.
func (dataStore *DataStore) Delete(key []byte) error {
	_, _, err := dataStore.fileManager.Write(key, nil, true)
	dataStore.keydir.DeleteRecord(key)
	return err
}

// ListKeys returns a list of all keys in the datastore. Note: This is intended to be
// used for debug or inspection.
func (dataStore *DataStore) ListKeys() ([]string, error) {
	return dataStore.keydir.GetAllKeys(), nil
}

func (dataStore *DataStore) Merge(directoryPath string) error {
	// nop
	return nil
}

func (dataStore *DataStore) Sync() error {
	return dataStore.fileManager.Sync()
}

// Size returns the number of keys present in the datastore
func (dataStore *DataStore) Size() int {
	return dataStore.keydir.Size()
}

// Close closes the datastore, writes pending changes (if any), and frees resources
func (dataStore *DataStore) Close() error {
	if err := dataStore.fileManager.Sync(); err != nil {
		return err
	}
	err1 := dataStore.fileManager.Close()
	if err1 != nil {
		return err1
	}
	return nil
}
