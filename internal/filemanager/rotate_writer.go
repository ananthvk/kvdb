package filemanager

import (
	"time"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/spf13/afero"
)

// RotateWriter writes data to a set of files. It changes the file to be written if
// the size of the current file exceeds the set limit. This struct and it's associated methods
// are not safe for concurrent use, and does not implement any locking
type RotateWriter struct {
	fs              afero.Fs
	writer          *record.Writer
	maxDatafileSize int
	currentFilePath string
	shouldRotate    bool

	// Callback function to get the next file path
	// This function is called when the writer wants to rotate to the next file
	getNextFilePath func() string
}

func (r *RotateWriter) Sync() error {
	if r.writer != nil {
		return r.writer.Sync()
	}
	return nil
}

func (r *RotateWriter) Close() error {
	var err error
	if r.writer != nil {
		err = r.writer.Close()
		r.writer = nil
	}
	return err
}

// Write Returns file path, offset (from start of file), error if any
func (r *RotateWriter) Write(key []byte, value []byte, isTombstone bool) (string, int64, error) {
	if r.shouldRotate || r.writer == nil {
		if err := r.getNewWriter(); err != nil {
			return r.currentFilePath, 0, err
		}
	}
	r.shouldRotate = false
	var offset int64
	var err error
	if isTombstone {
		offset, err = r.writer.WriteTombstone(key)
	} else {
		offset, err = r.writer.WriteKeyValue(key, value)
	}
	if err != nil {
		return r.currentFilePath, 0, err
	}
	if offset > int64(r.maxDatafileSize) {
		r.shouldRotate = true
	}
	return r.currentFilePath, offset, nil
}

// Write Returns file path, offset (from start of file), error if any (with timestamp), Note: Quick hack, I've just copied this function
func (r *RotateWriter) WriteWithTs(key []byte, value []byte, isTombstone bool, ts time.Time) (string, int64, error) {
	if r.shouldRotate || r.writer == nil {
		if err := r.getNewWriter(); err != nil {
			return r.currentFilePath, 0, err
		}
	}
	r.shouldRotate = false
	var offset int64
	var err error
	if isTombstone {
		offset, err = r.writer.WriteTombstoneWithTs(key, ts)
	} else {
		offset, err = r.writer.WriteKeyValueWithTs(key, value, ts)
	}
	if err != nil {
		return r.currentFilePath, 0, err
	}
	if offset > int64(r.maxDatafileSize) {
		r.shouldRotate = true
	}
	return r.currentFilePath, offset, nil
}

func (r *RotateWriter) getNewWriter() error {
	if r.writer != nil {
		if err := r.writer.Sync(); err != nil {
			return err
		}
		if err := r.writer.Close(); err != nil {
			return err
		}
		r.writer = nil
	}
	r.currentFilePath = r.getNextFilePath()
	err := datafile.WriteFileHeader(r.fs, r.currentFilePath, time.Now())
	if err != nil {
		return err
	}
	writer, err := record.NewWriter(r.fs, r.currentFilePath)
	if err != nil {
		return err
	}
	r.writer = writer
	return nil
}

// NewRotateWriter creates a new instance of RotateWriter with the specified parameters.
func NewRotateWriter(fs afero.Fs, maxDatafileSize int, getNextFilePath func() string) *RotateWriter {
	return &RotateWriter{
		fs:              fs,
		maxDatafileSize: maxDatafileSize,
		getNextFilePath: getNextFilePath,
		shouldRotate:    false,
	}
}
