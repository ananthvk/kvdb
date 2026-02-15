package kvdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/filemanager"
	"github.com/ananthvk/kvdb/internal/hintfile"
	"github.com/ananthvk/kvdb/internal/keydir"
	"github.com/ananthvk/kvdb/internal/metafile"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/ananthvk/kvdb/internal/utils"
	"github.com/spf13/afero"
)

type DataStore struct {
	fs          afero.Fs
	path        string
	metaInfo    *metafile.MetaData
	keydir      *keydir.Keydir
	fileManager *filemanager.FileManager
	mu          sync.RWMutex
	// To ensure that only one merge can occur at a time
	mergeLock sync.Mutex
}

const (
	datastoreType          = "kvdb"            // Type of store
	version                = "1.0.0"           // Version of the application
	defaultMaxDatafileSize = 128 * 1000 * 1000 // In bytes (128 MB)
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

	// Make the hint/ folder
	if err := fs.Mkdir(filepath.Join(path, "hint"), os.ModePerm); err != nil {
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

	fm, err := filemanager.NewFileManager(fs, path, metainfo.MaxDatafileSize)
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
	dataStore.mu.RLock()
	defer dataStore.mu.RUnlock()
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
	dataStore.mu.Lock()
	defer dataStore.mu.Unlock()
	fileId, offset, err := dataStore.fileManager.Write(key, value, false)
	if err != nil {
		return err
	}
	dataStore.keydir.AddKeydirRecord(key, fileId, uint32(len(value)), offset-datafile.FileHeaderSize, time.Now())
	return err
}

// Delete deletes the value associated with the specified key. No error will be returned if the key does not exist.
// An error is returned if the deletion failed due to some other reason.
func (dataStore *DataStore) Delete(key []byte) error {
	dataStore.mu.Lock()
	defer dataStore.mu.Unlock()
	_, _, err := dataStore.fileManager.Write(key, nil, true)
	if err != nil {
		return err
	}
	dataStore.keydir.DeleteRecord(key)
	return err
}

// ListKeys returns a list of all keys in the datastore. Note: This is intended to be
// used for debug or inspection.
func (dataStore *DataStore) ListKeys() ([]string, error) {
	dataStore.mu.RLock()
	defer dataStore.mu.RUnlock()
	return dataStore.keydir.GetAllKeys(), nil
}
func (dataStore *DataStore) Merge() error {
	dataStore.mergeLock.Lock()
	defer dataStore.mergeLock.Unlock()
	immutableFiles, err := dataStore.fileManager.GetImmutableFiles()
	if err != nil {
		return err
	}

	type valueLoc struct {
		path         string
		offset       int64
		ts           time.Time
		sourceFileId int
	}
	valueLocations := map[string]valueLoc{}
	mergeWriter, err := dataStore.fileManager.NewMergeWriter()
	if err != nil {
		return err
	}
	defer mergeWriter.Close()

	var currentHintWriter *hintfile.Writer
	var lastDataFilePath string = ""

	for _, dataFile := range immutableFiles {
		filePath := filepath.Join(dataStore.path, "data", utils.GetDataFileName(dataFile))
		scanner, err := record.NewScanner(dataStore.fs, filePath)
		if err != nil {
			// TODO: Skip this file from merge
			fmt.Fprintf(os.Stderr, "Could not open file with id %d for merging\n", dataFile)
			continue
		}

		for {
			rec, offset, err := scanner.Scan()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				// TODO: Skip this file
				return err
			}

			// Check if the record is active
			var exists bool
			var kdRecord keydir.KeydirRecord

			dataStore.mu.RLock()
			kdRecord, exists = dataStore.keydir.GetKeydirRecord(rec.Key)
			dataStore.mu.RUnlock()

			// This record is stale, skip it
			if !exists || kdRecord.FileId != dataFile || kdRecord.ValuePos != offset {
				continue
			}

			if rec.Header.RecordType == record.RecordTypeDelete {
				// Ignore tombstones
				continue
			}

			filePath, newPos, err := mergeWriter.WriteWithTs(rec.Key, rec.Value, false, rec.Header.Timestamp)
			if err != nil {
				return err
			}

			// If the file path has changed, we need to create a new hint file writer
			if filePath != lastDataFilePath {
				if currentHintWriter != nil {
					currentHintWriter.Close()
				}
				hintPath := filepath.Join(dataStore.path, "hint", filepath.Base(filePath))
				currentHintWriter, err = hintfile.NewWriter(dataStore.fs, hintPath)
				if err != nil {
					return err
				}
				lastDataFilePath = filePath
			}

			// Write to hint file
			err = currentHintWriter.WriteHintRecord(&hintfile.HintRecord{
				Timestamp: rec.Header.Timestamp,
				KeySize:   rec.Header.KeySize,
				ValueSize: rec.Header.ValueSize,
				ValuePos:  newPos,
				Key:       rec.Key,
			})
			if err != nil {
				return err
			}

			valueLocations[string(rec.Key)] = valueLoc{
				path:         filePath,
				offset:       newPos,
				ts:           rec.Header.Timestamp,
				sourceFileId: dataFile,
			}
		}
		scanner.Close()
	}

	if currentHintWriter != nil {
		currentHintWriter.Close()
	}

	// TODO: fsync the directory (after rename)
	mergeWriter.Sync()
	mergeWriter.Close()

	tempFilesList := mergeWriter.GetFilePaths()

	// Get the write lock, reserve the file Ids
	dataStore.mu.Lock()
	startId := dataStore.fileManager.IncrementNextDataFileNumber(len(tempFilesList))
	dataStore.mu.Unlock()

	// Now, rename all temporary files starting from startId
	// Also rename hint files
	realFileIds := make(map[string]int)
	for i, mergeFilePath := range tempFilesList {
		realId := startId + i
		dataStore.fs.Rename(mergeFilePath, filepath.Join(dataStore.path, "data", utils.GetDataFileName(realId)))

		hintPath := filepath.Join(dataStore.path, "hint", filepath.Base(mergeFilePath))
		dataStore.fs.Rename(hintPath, filepath.Join(dataStore.path, "hint", utils.GetHintFileName(realId)))

		// To be used when updating keydir
		realFileIds[mergeFilePath] = realId
	}

	// Get the write lock, and update keydir with new Ids
	dataStore.mu.Lock()

	for key, loc := range valueLocations {
		// Only update if the key in keydir is still pointing to old file (i.e. the value has not been updated)
		keyBytes := []byte(key)
		current, exists := dataStore.keydir.GetKeydirRecord(keyBytes)
		if exists && current.FileId == loc.sourceFileId {
			realID := realFileIds[loc.path]
			dataStore.keydir.AddKeydirRecord(keyBytes, realID, current.ValueSize, loc.offset-datafile.FileHeaderSize, current.Timestamp)
		}
	}
	dataStore.mu.Unlock()

	// Delete old immutable files & hints
	for _, dataFile := range immutableFiles {
		filePath := filepath.Join(dataStore.path, "data", utils.GetDataFileName(dataFile))
		hintFilePath := filepath.Join(dataStore.path, "hint", utils.GetHintFileName(dataFile))
		dataStore.fs.Remove(filePath)
		dataStore.fs.Remove(hintFilePath)
	}

	dataStore.fileManager.CloseAndDeleteReaders(immutableFiles)

	return nil
}

func (dataStore *DataStore) Sync() error {
	dataStore.mu.Lock()
	defer dataStore.mu.Unlock()
	return dataStore.fileManager.Sync()
}

// Size returns the number of keys present in the datastore
func (dataStore *DataStore) Size() int {
	dataStore.mu.RLock()
	defer dataStore.mu.RUnlock()
	return dataStore.keydir.Size()
}

// Close closes the datastore, writes pending changes (if any), and frees resources
func (dataStore *DataStore) Close() error {
	dataStore.mu.Lock()
	defer dataStore.mu.Unlock()
	if err := dataStore.fileManager.Sync(); err != nil {
		return err
	}
	err1 := dataStore.fileManager.Close()
	if err1 != nil {
		return err1
	}
	return nil
}
