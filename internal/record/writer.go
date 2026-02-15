package record

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/spf13/afero"
)

// Writer is responsible for writing log records to the file. There are no locks in this implementation, so it's
// unsafe to call Writer methods concurrently
type Writer struct {
	fs   afero.Fs
	file afero.File
	// Internal buffer used to temporarily hold record header
	buf        [recordHeaderSize]byte
	currentPos int64
}

// NewWriter creates a new Record Writer that opens a file at the specified path for appending logs
func NewWriter(fs afero.Fs, path string) (*Writer, error) {
	file, err := fs.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}

	// Seek to end to find the size of the file (position for the next record)
	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Writer{
		fs:         fs,
		file:       file,
		currentPos: pos,
	}, nil
}

// writeRecord writes the key-value record to the file. It writes the record header, followed by the key & value, then the CRC checksum
func (w *Writer) writeRecord(r *Record) error {
	h := crc32.NewIEEE()
	// Set header fields
	binary.LittleEndian.PutUint64(w.buf[0:], uint64(r.Header.Timestamp.UnixMicro())) // Unix timestamp (in microseconds)
	binary.LittleEndian.PutUint32(w.buf[8:], r.Header.KeySize)                       // Length of key
	binary.LittleEndian.PutUint32(w.buf[12:], r.Header.ValueSize)                    // Length of value
	w.buf[16] = r.Header.RecordType                                                  // Type of record, 0x50 for PUT, and 0x44 for DELETE
	w.buf[17] = r.Header.ValueType                                                   // Currently value type is unused
	w.buf[18] = 0x0                                                                  // Reserved
	w.buf[19] = 0x0                                                                  // Reserved

	// Update CRC with header info
	h.Write(w.buf[:])
	if _, err := w.file.Write(w.buf[:]); err != nil {
		return err
	}

	// Update CRC with key & value
	h.Write(r.Key)
	if _, err := w.file.Write(r.Key); err != nil {
		return err
	}
	h.Write(r.Value)
	if _, err := w.file.Write(r.Value); err != nil {
		return err
	}

	// Write the CRC of the record at the end
	crc := h.Sum32()
	if err := binary.Write(w.file, binary.LittleEndian, crc); err != nil {
		return err
	}
	w.currentPos += int64(r.Size)
	return nil
}

// WriteKeyValue writes the key-value pair as a new log entry to the file. It does not call sync(), so there
// is a chance that data might get lost if the system crashes. If you need durability, call Sync() after writing.
// This function returns the offset of the record in the file, measured from the start of the file
func (w *Writer) WriteKeyValue(key []byte, value []byte) (int64, error) {
	start := w.currentPos
	rec := newRecord(key, value, recordTypePut)
	return start, w.writeRecord(rec)
}

// WriteTombstone writes a tombstone value for the specified key, this function is to be used
// to delete a key from the store.
// This function returns the offset of the record in the file, measured from the start of the file
func (w *Writer) WriteTombstone(key []byte) (int64, error) {
	start := w.currentPos
	rec := newRecord(key, nil, RecordTypeDelete)
	return start, w.writeRecord(rec)
}

func (w *Writer) WriteKeyValueWithTs(key []byte, value []byte, ts time.Time) (int64, error) {
	start := w.currentPos
	rec := newRecord(key, value, recordTypePut)
	rec.Header.Timestamp = ts
	return start, w.writeRecord(rec)
}

func (w *Writer) WriteTombstoneWithTs(key []byte, ts time.Time) (int64, error) {
	start := w.currentPos
	rec := newRecord(key, nil, RecordTypeDelete)
	rec.Header.Timestamp = ts
	return start, w.writeRecord(rec)
}

// Sync flushes any buffered data to the underlying file. It calls sync() on the file
func (w *Writer) Sync() error {
	return w.file.Sync()
}

// Close closes the underlying file, it also writes any pending changes and syncs the changes to the disk
func (w *Writer) Close() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
