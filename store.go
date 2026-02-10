package kvdb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/metafile"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/ananthvk/kvdb/internal/utils"
	"github.com/spf13/afero"
)

// No Keydir for now, just do a linear scan of the entire file

type DataStore struct {
	fs       afero.Fs
	path     string
	metaInfo *metafile.MetaData
	reader   *record.Reader
	writer   *record.Writer
}

const (
	datastoreType             = "kvdb"            // Type of store
	version                   = "1.0.0"           // Version of the application
	default_max_key_size      = 128               // In bytes (128 bytes)
	default_max_value_size    = 64000             // In bytes (64 KB)
	default_max_datafile_size = 100 * 1000 * 1000 // In bytes (100 MB)
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
		MaxKeySize:      default_max_key_size,
		MaxValueSize:    default_max_value_size,
		MaxDatafileSize: default_max_datafile_size,
	}
	// Write the metafile
	if err := metafile.WriteMetaFile(fs, path, metainfo); err != nil {
		return nil, err
	}

	// Make the data/ folder
	if err := fs.Mkdir(filepath.Join(path, "data"), os.ModePerm); err != nil {
		return nil, err
	}

	// Write the first file
	firstFile := utils.GetDataFileName(1)

	path = filepath.Join(path, "data", firstFile)

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
		fs:       fs,
		path:     path,
		metaInfo: metainfo,
		reader:   reader,
		writer:   writer,
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

	firstFile := utils.GetDataFileName(1)
	path = filepath.Join(path, "data", firstFile)

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
		fs:       fs,
		path:     path,
		metaInfo: metainfo,
		reader:   reader,
		writer:   writer,
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
		rec, err := dataStore.reader.ReadRecordAt(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if len(rec.Key) == len(key) && string(rec.Key) == string(key) {
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
