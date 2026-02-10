package filemanager

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/keydir"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/ananthvk/kvdb/internal/utils"
	"github.com/spf13/afero"
)

type FileManager struct {
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
			fmt.Println("Found datafile", i)
		}
	}

	// If there is no datafile, create one
	if maxDatafileNumber == 0 {
		maxDatafileNumber = 1
		dataFileName := utils.GetDataFileName(maxDatafileNumber)
		// Write the file header
		err := datafile.WriteFileHeader(fs, filepath.Join(dataDirPath, dataFileName), datafile.NewFileHeader(time.Now(), 0))
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
	if f.isWriterClosed {
		// Create the new data file
		dataFileName := utils.GetDataFileName(f.nextDataFileNumber)
		// Write the file header
		err := datafile.WriteFileHeader(f.fs,
			filepath.Join(f.dataStoreRootPath, "data", dataFileName),
			datafile.NewFileHeader(time.Now(), 0),
		)
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
	if reader, exists := f.readers[fileId]; exists {
		return reader.ReadRecordAtStrict(offset)
	}

	dataFileName := utils.GetDataFileName(fileId)
	reader, err := record.NewReader(f.fs, filepath.Join(f.dataStoreRootPath, "data", dataFileName), datafile.FileHeaderSize)
	if err != nil {
		return nil, err
	}

	f.readers[fileId] = reader
	return reader.ReadRecordAtStrict(offset)
}

// ReadValueAt reads the value at a specific offset in the data file.
// It caches the reader in the map for future use.
func (f *FileManager) ReadValueAt(fileId int, offset int64) (*record.Record, error) {
	if reader, exists := f.readers[fileId]; exists {
		return reader.ReadValueAt(offset)
	}
	dataFileName := utils.GetDataFileName(fileId)
	reader, err := record.NewReader(f.fs, filepath.Join(f.dataStoreRootPath, "data", dataFileName), datafile.FileHeaderSize)
	if err != nil {
		return nil, err
	}

	f.readers[fileId] = reader
	return reader.ReadValueAt(offset)
}

func (f *FileManager) ReadKeydir() (*keydir.Keydir, error) {
	dataDirPath := filepath.Join(f.dataStoreRootPath, "data")
	entries, err := afero.ReadDir(f.fs, dataDirPath)
	kd := keydir.NewKeydir()
	if err != nil {
		return nil, err
	}
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

			reader, err := record.NewReader(f.fs, datafilePath, datafile.FileHeaderSize)
			if err != nil {
				fmt.Printf("build keydir, skip %s, error: %s\n", filename, err)
				continue
			}
			err = f.addRecordsToKeydir(kd, int(fileId), reader)
			if err != nil {
				fmt.Printf("build keydir, %s error: %s\n", filename, err)
			}
		}
	}
	return kd, nil
}

func (f *FileManager) Sync() error {
	return f.writer.Sync()
}

func (f *FileManager) Close() error {
	return f.writer.Close()
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
