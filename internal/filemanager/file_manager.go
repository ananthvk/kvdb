package filemanager

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/keydir"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/ananthvk/kvdb/internal/utils"
	"github.com/spf13/afero"
)

type FileManager struct {
	mu                 sync.RWMutex
	fs                 afero.Fs
	dataStoreRootPath  string
	readers            map[int]*record.Reader
	rotateWriter       *RotateWriter
	activeDataFile     int
	nextDataFileNumber int
}

func NewFileManager(fs afero.Fs, path string, maxDatafileSize int) (*FileManager, error) {
	// In ${root}/data directory, find the file with the numerical maximum value, and open it for writing
	// If the file is not a data file, it'll be skipped
	dataDirPath := filepath.Join(path, "data")
	entries, err := afero.ReadDir(fs, dataDirPath)
	if err != nil {
		return nil, err
	}
	maxDatafileNumber := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			filename := entry.Name()
			extension := filepath.Ext(filename)
			nameWithoutExt := strings.TrimSuffix(filename, extension)
			i, err := strconv.ParseInt(nameWithoutExt, 10, 32)
			if err != nil {
				continue
			}

			// Note: This may or may not be the latest active file
			// but it doesn't matter in this case (except the case of crash recover)
			maxDatafileNumber = max(maxDatafileNumber, int(i))
		}
	}

	// TODO: Implement crash recovery & check to see if it has exceeded max size
	fileManager := &FileManager{
		fs:                 fs,
		dataStoreRootPath:  path,
		readers:            map[int]*record.Reader{},
		activeDataFile:     maxDatafileNumber,
		nextDataFileNumber: maxDatafileNumber + 1,
	}

	fileManager.rotateWriter = NewRotateWriter(fs, maxDatafileSize, func() string {
		dataFileName := utils.GetDataFileName(fileManager.nextDataFileNumber)
		// Note: Because of this, each time a restart happens, a new file will be created
		// And all previous files will be treated as immutable
		// This is safer for crash recovery, but it's not efficient since a new file is created on every restart
		// TODO: Fix this later
		fileManager.activeDataFile = fileManager.nextDataFileNumber
		fileManager.nextDataFileNumber++
		return filepath.Join(dataDirPath, dataFileName)
	})

	return fileManager, nil
}

// WriteKeyValue Returns fileId, offset (from start of file), error if any
func (f *FileManager) Write(key []byte, value []byte, isTombstone bool) (int, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, offset, err := f.rotateWriter.Write(key, value, isTombstone)
	return f.activeDataFile, offset, err
}

// ReadRecordAtStrict reads a record at a specific offset in the data file.
// It caches the reader in the map for future use.
func (f *FileManager) ReadRecordAtStrict(fileId int, offset int64) (*record.Record, error) {
	reader, err := f.getReader(fileId)
	if err != nil {
		return nil, err
	}
	return reader.ReadRecordAtStrict(offset)
}

// ReadValueAt reads the value at a specific offset in the data file.
// It caches the reader in the map for future use.
func (f *FileManager) ReadValueAt(fileId int, offset int64) (*record.Record, error) {
	reader, err := f.getReader(fileId)
	if err != nil {
		return nil, err
	}
	return reader.ReadValueAt(offset)
}

func (f *FileManager) ReadKeydir() (*keydir.Keydir, error) {
	dataDirPath := filepath.Join(f.dataStoreRootPath, "data")
	kd := keydir.NewKeydir()
	ids, err := f.getSortedDataFileIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		fileName := utils.GetDataFileName(id)
		datafilePath := filepath.Join(dataDirPath, fileName)

		// Check if it's a datafile
		if _, err := datafile.ReadFileHeader(f.fs, datafilePath); err != nil {
			fmt.Printf("build keydir, skip %s, error: %s\n", fileName, err)
			continue
		}

		reader, err := record.NewReader(f.fs, datafilePath)
		if err != nil {
			fmt.Printf("build keydir, skip %s, error: %s\n", fileName, err)
			continue
		}

		fmt.Println("Load datafile", fileName)
		err = f.addRecordsToKeydir(kd, id, reader)
		if err != nil {
			fmt.Printf("build keydir, %s error: %s\n", fileName, err)
		}
		reader.Close()
	}
	return kd, nil
}

func (f *FileManager) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rotateWriter.Sync()
}

func (f *FileManager) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.rotateWriter.Close(); err != nil {
		return err
	}
	for _, reader := range f.readers {
		reader.Close()
	}

	return nil
}

func (f *FileManager) addRecordsToKeydir(kd *keydir.Keydir, fileId int, reader *record.Reader) error {
	var offset int64 = 0
	for {
		rec, err := reader.ReadRecordAtStrict(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if rec.Header.RecordType == record.RecordTypeDelete {
			kd.DeleteRecord(rec.Key)
		} else {
			kd.AddKeydirRecord(rec.Key, fileId, rec.Header.ValueSize, offset, rec.Header.Timestamp)
		}
		offset += rec.Size
	}
	return nil
}

// Use Double-Checked locking to create / return cached reader
func (f *FileManager) getReader(fileId int) (*record.Reader, error) {
	// Check if reader already exists
	f.mu.RLock()
	reader, exists := f.readers[fileId]
	f.mu.RUnlock()
	if exists {
		return reader, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// Reader does not exist, update cache by creating a reader
	if reader, exists := f.readers[fileId]; exists {
		// Some other goroutine has created a reader before this thread acquired the lock
		return reader, nil
	}

	dataFileName := utils.GetDataFileName(fileId)
	reader, err := record.NewReader(f.fs, filepath.Join(f.dataStoreRootPath, "data", dataFileName))
	if err != nil {
		return nil, err
	}
	f.readers[fileId] = reader
	return reader, nil
}

// GetImmutableFiles returns a list of integer Ids for immutable files in
// the given data store
func (f *FileManager) GetImmutableFiles() ([]int, error) {
	f.mu.RLock()
	snapshotActiveID := f.activeDataFile
	f.mu.RUnlock()
	ids, err := f.getSortedDataFileIDs()
	if err != nil {
		return nil, err
	}
	immutableIds := make([]int, 0, len(ids))
	for _, id := range ids {
		// If the fileId is > the last active file ID, a rotation could have
		// happened, so skip this file for now, and consider it in the next cycle
		if id < snapshotActiveID {
			// This is because it's guaranteed that files with ID less < snapshot ID
			// are all immutable
			immutableIds = append(immutableIds, id)
		}
	}
	return immutableIds, nil
}

func (f *FileManager) getSortedDataFileIDs() ([]int, error) {
	entries, err := afero.ReadDir(f.fs, filepath.Join(f.dataStoreRootPath, "data"))
	if err != nil {
		return nil, err
	}

	var ids []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		filename := entry.Name()
		extension := filepath.Ext(filename)
		nameWithoutExt := strings.TrimSuffix(filename, extension)
		fileId, err := strconv.ParseInt(nameWithoutExt, 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, int(fileId))
	}
	sort.Ints(ids)
	return ids, nil
}

type MergeWriter struct {
	fs            afero.Fs
	directoryPath string
}

func (m *MergeWriter) Write() {
}

func (f *FileManager) NewMergeWriter() (*MergeWriter, error) {
	return &MergeWriter{
		fs:            f.fs,
		directoryPath: filepath.Join(f.dataStoreRootPath, "data"),
	}, nil
}
