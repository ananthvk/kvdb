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
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/keydir"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/ananthvk/kvdb/internal/utils"
	"github.com/spf13/afero"
)

const mergePrefix = "merge"

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

	fileManager.rotateWriter = NewRotateWriter(fs, maxDatafileSize, false, func() string {
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
	reader, err := f.GetReader(fileId)
	if err != nil {
		return nil, err
	}
	return reader.ReadRecordAtStrict(offset)
}

// ReadValueAt reads the value at a specific offset in the data file.
// It caches the reader in the map for future use.
func (f *FileManager) ReadValueAt(fileId int, offset int64) (*record.Record, error) {
	reader, err := f.GetReader(fileId)
	if err != nil {
		return nil, err
	}
	return reader.ReadValueAt(offset)
}

// CloseAndDeleteReaders closes and deletes the readers for the given list of IDs.
func (f *FileManager) CloseAndDeleteReaders(ids []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		if reader, exists := f.readers[id]; exists {
			reader.Close()
			delete(f.readers, id)
		}
	}
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

		err = f.addRecordsToKeydir(kd, id)
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

func (f *FileManager) addRecordsToKeydir(kd *keydir.Keydir, fileId int) error {
	scanner, err := record.NewScanner(f.fs, filepath.Join(f.dataStoreRootPath, "data", utils.GetDataFileName(fileId)))
	if err != nil {
		return err
	}
	defer scanner.Close()
	for {
		rec, offset, err := scanner.Scan()
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
	}
	return nil
}

// Use Double-Checked locking to create / return cached reader
func (f *FileManager) GetReader(fileId int) (*record.Reader, error) {
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

// IncrementNextDataFileNumber increments the next data file number by the specified value.
// It returns the value of nextDataFileNumber before the increment
func (f *FileManager) IncrementNextDataFileNumber(n int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	nextDataFileNumber := f.nextDataFileNumber
	f.nextDataFileNumber += n
	return nextDataFileNumber
}

// Note: Does not lock rotateWriter internally and is hence unsafe for concurrent use
type MergeWriter struct {
	fs            afero.Fs
	directoryPath string
	rotateWriter  *RotateWriter
	filePaths     []string
}

// Returns filePath, offset, error (if any)
func (m *MergeWriter) Write(key []byte, value []byte, isTombstone bool) (string, int64, error) {
	return m.rotateWriter.Write(key, value, isTombstone)
}

func (m *MergeWriter) WriteWithTs(key []byte, value []byte, isTombstone bool, timestamp time.Time) (string, int64, error) {
	return m.rotateWriter.WriteWithTs(key, value, isTombstone, timestamp)
}

func (m *MergeWriter) Sync() error {
	return m.rotateWriter.Sync()
}

func (m *MergeWriter) Close() error {
	return m.rotateWriter.Close()
}

// NewMergeWriter returns a merge writer. Note: Then underlying RotateWriter is opened in buffered mode to improve performance
// So, Sync() is mandatory to write contents of file to disk
func (f *FileManager) NewMergeWriter() (*MergeWriter, error) {
	counter := 0
	mergeWriter := &MergeWriter{
		fs:            f.fs,
		directoryPath: filepath.Join(f.dataStoreRootPath, "data"),
	}
	rotateWriter := NewRotateWriter(f.fs, f.rotateWriter.maxDatafileSize, true, func() string {
		counter++
		dataFilePath := filepath.Join(mergeWriter.directoryPath, fmt.Sprintf("%s-%d", mergePrefix, counter))
		mergeWriter.filePaths = append(mergeWriter.filePaths, dataFilePath)
		return dataFilePath
	})
	mergeWriter.rotateWriter = rotateWriter
	return mergeWriter, nil
}

// Returns a list of paths of all files created by this merge writer
func (m *MergeWriter) GetFilePaths() []string {
	return m.filePaths
}
