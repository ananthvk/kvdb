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

type FileManager struct {
	mu                 sync.RWMutex
	fs                 afero.Fs
	dataStoreRootPath  string
	readers            map[int]*record.Reader
	writer             *record.Writer
	maxDatafileSize    int
	nextDataFileNumber int
	isWriterClosed     bool
}

func NewFileManager(fs afero.Fs, path string, maxDatafileSize int) (*FileManager, error) {
	// In ${root}/data directory, find the file with the numerical maximum value, and open it for writing
	// If such a file does not exist, a new file will be created
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

			maxDatafileNumber = max(maxDatafileNumber, int(i))
		}
	}

	// If there is no datafile, create one
	if maxDatafileNumber == 0 {
		maxDatafileNumber = 1
		dataFileName := utils.GetDataFileName(maxDatafileNumber)
		// Write the file header
		err := datafile.WriteFileHeader(fs, filepath.Join(dataDirPath, dataFileName), time.Now())
		if err != nil {
			return nil, err
		}
	}

	// TODO: Implement crash recovery & check to see if it has exceeded max size

	dataFileName := utils.GetDataFileName(maxDatafileNumber)
	writer, err := record.NewWriter(fs, filepath.Join(dataDirPath, dataFileName))
	if err != nil {
		return nil, err
	}

	return &FileManager{
		fs:                 fs,
		dataStoreRootPath:  path,
		maxDatafileSize:    maxDatafileSize,
		readers:            map[int]*record.Reader{},
		nextDataFileNumber: maxDatafileNumber + 1,
		writer:             writer,
	}, nil
}

// WriteKeyValue Returns fileId, offset (from start of file), error if any
func (f *FileManager) Write(key []byte, value []byte, isTombstone bool) (int, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.isWriterClosed {
		// Create the new data file
		dataFileName := utils.GetDataFileName(f.nextDataFileNumber)
		// Write the file header
		err := datafile.WriteFileHeader(f.fs,
			filepath.Join(f.dataStoreRootPath, "data", dataFileName), time.Now())
		if err != nil {
			return 0, 0, err
		}

		writer, err := record.NewWriter(f.fs, filepath.Join(f.dataStoreRootPath, "data", dataFileName))
		if err != nil {
			return 0, 0, err
		}

		f.writer = writer
		f.nextDataFileNumber++
		f.isWriterClosed = false
	}

	fileId := f.nextDataFileNumber - 1
	var offset int64
	var err error
	if isTombstone {
		offset, err = f.writer.WriteTombstone(key)
	} else {
		offset, err = f.writer.WriteKeyValue(key, value)
	}

	if err != nil {
		return 0, 0, err
	}
	// Check if we need to split here, and start writing to the next file
	if offset > int64(f.maxDatafileSize) {
		f.isWriterClosed = true
		f.writer.Sync()
		f.writer.Close()
	}
	return fileId, offset, nil
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
	entries, err := afero.ReadDir(f.fs, dataDirPath)
	kd := keydir.NewKeydir()
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir() || entries[j].IsDir() {
			return false
		}
		nameI := strings.TrimSuffix(entries[i].Name(), filepath.Ext(entries[i].Name()))
		nameJ := strings.TrimSuffix(entries[j].Name(), filepath.Ext(entries[j].Name()))
		numI, _ := strconv.Atoi(nameI)
		numJ, _ := strconv.Atoi(nameJ)
		return numI < numJ
	})
	for _, entry := range entries {
		if !entry.IsDir() {
			filename := entry.Name()
			extension := filepath.Ext(filename)
			nameWithoutExt := strings.TrimSuffix(filename, extension)
			fileId, err := strconv.ParseInt(nameWithoutExt, 10, 32)
			if err != nil {
				continue
			}

			datafilePath := filepath.Join(dataDirPath, filename)
			// Check if it's a datafile
			if _, err := datafile.ReadFileHeader(f.fs, datafilePath); err != nil {
				fmt.Printf("build keydir, skip %s, error: %s\n", filename, err)
				continue
			}

			reader, err := record.NewReader(f.fs, datafilePath)
			if err != nil {
				fmt.Printf("build keydir, skip %s, error: %s\n", filename, err)
				continue
			}

			fmt.Println("Load datafile", entry.Name())
			err = f.addRecordsToKeydir(kd, int(fileId), reader)
			if err != nil {
				fmt.Printf("build keydir, %s error: %s\n", filename, err)
			}
		}
	}
	return kd, nil
}

func (f *FileManager) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writer.Sync()
}

func (f *FileManager) Close() error {
	f.mu.Lock()
	if f.writer != nil {
		if err := f.writer.Close(); err != nil {
			return err
		}
	}

	for id, reader := range f.readers {
		if err := reader.Close(); err != nil {
			// TODO: Log it later
			continue
		}
		delete(f.readers, id)
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
