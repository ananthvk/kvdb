package record

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"time"

	"github.com/ananthvk/kvdb/internal/constants"
	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/spf13/afero"
)

// Reader is responsible for reading log records from a file. This implementation uses ReadAt (that uses pread internally on supported files)
// and hence is safe to access concurrently
type Reader struct {
	fs   afero.Fs
	file afero.File
}

// NewReader creates a new Record Reader that opens a file at the specified path for reading log records.
// It starts reading from the 19th byte in the file (To skip the header)
func NewReader(fs afero.Fs, path string) (*Reader, error) {
	file, err := fs.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &Reader{
		fs:   fs,
		file: file,
	}, nil
}

// ReadValueAt reads a record at the given offset (from the start of the first record).
// It only reads and populates the value in the returned record. Key is left empty.
func (r *Reader) ReadValueAt(offset int64) (*Record, error) {
	currentOffset := offset + datafile.FileHeaderSize
	header, err := r.readHeader(nil, currentOffset)
	if err != nil {
		return nil, err
	}
	currentOffset += recordHeaderSize
	record := &Record{
		Header: *header,
		Value:  make([]byte, header.ValueSize),
		Size:   int64(recordHeaderSize + header.KeySize + header.ValueSize + 4),
	}
	// Skip over the key
	currentOffset += int64(header.KeySize)
	n, err := r.file.ReadAt(record.Value, currentOffset)
	if err != nil {
		return nil, err
	}
	if n != int(header.ValueSize) {
		return nil, fmt.Errorf("expected to read %d bytes for value, got %d", header.ValueSize, n)
	}
	return record, nil
}

// ReadKeyAt reads a record at the given offset (from the start of the first record).
// It only reads and populates the key in the returned record. Value is left empty.
func (r *Reader) ReadKeyAt(offset int64) (*Record, error) {
	currentOffset := offset + datafile.FileHeaderSize
	header, err := r.readHeader(nil, currentOffset)
	if err != nil {
		return nil, err
	}
	currentOffset += recordHeaderSize
	record := &Record{
		Header: *header,
		Key:    make([]byte, header.KeySize),
		Size:   int64(recordHeaderSize + header.KeySize + header.ValueSize + 4),
	}
	n, err := r.file.ReadAt(record.Key, currentOffset)
	if err != nil {
		return nil, err
	}
	if n != int(header.KeySize) {
		return nil, fmt.Errorf("expected to read %d bytes for key, got %d", header.KeySize, n)
	}
	return record, nil
}

// ReadRecordAt reads a record at the given offset (from the start of the first record).
// It reads both the key and value from the file, and both the Key and Value in the returned record are valid.
func (r *Reader) ReadRecordAt(offset int64) (*Record, error) {
	currentOffset := offset + datafile.FileHeaderSize
	header, err := r.readHeader(nil, currentOffset)
	if err != nil {
		return nil, err
	}
	currentOffset += recordHeaderSize
	record := &Record{
		Header: *header,
		Key:    make([]byte, header.KeySize),
		Value:  make([]byte, header.ValueSize),
		Size:   int64(recordHeaderSize + header.KeySize + header.ValueSize + 4),
	}

	n, err := r.file.ReadAt(record.Key, currentOffset)
	if err != nil {
		return nil, err
	}
	if n != int(header.KeySize) {
		return nil, fmt.Errorf("expected to read %d bytes for key, got %d", header.KeySize, n)
	}
	currentOffset += int64(header.KeySize)
	n, err = r.file.ReadAt(record.Value, currentOffset)
	if err != nil {
		return nil, err
	}
	if n != int(header.ValueSize) {
		return nil, fmt.Errorf("expected to read %d bytes for value, got %d", header.ValueSize, n)
	}
	return record, nil
}

// ReadRecordAtStrict reads a record at the given offset (from the start of the first record).
// It reads both the key and value from the file, and both the Key and Value in the returned record are valid.
// It also verifies if the record is valid by computing the CRC checksum
func (r *Reader) ReadRecordAtStrict(offset int64) (*Record, error) {
	currentOffset := offset + datafile.FileHeaderSize

	h := crc32.NewIEEE()
	header, err := r.readHeader(h, currentOffset)
	if err != nil {
		return nil, err
	}
	currentOffset += recordHeaderSize

	record := &Record{
		Header: *header,
		Key:    make([]byte, header.KeySize),
		Value:  make([]byte, header.ValueSize),
		Size:   int64(recordHeaderSize + header.KeySize + header.ValueSize + 4),
	}

	n, err := r.file.ReadAt(record.Key, currentOffset)
	if err != nil {
		return nil, err
	}
	if n != int(header.KeySize) {
		return nil, fmt.Errorf("expected to read %d bytes for key, got %d", header.KeySize, n)
	}
	currentOffset += int64(header.KeySize)
	h.Write(record.Key)

	n, err = r.file.ReadAt(record.Value, currentOffset)
	if err != nil {
		return nil, err
	}
	if n != int(header.ValueSize) {
		return nil, fmt.Errorf("expected to read %d bytes for value, got %d", header.ValueSize, n)
	}
	currentOffset += int64(record.Header.ValueSize)
	h.Write(record.Value)

	crc := h.Sum32()

	var buf [4]byte
	if _, err := r.file.ReadAt(buf[0:4], currentOffset); err != nil {
		return nil, err
	}

	fileCrc := binary.LittleEndian.Uint32(buf[0:4])

	if fileCrc != crc {
		return nil, ErrCrcChecksumMismatch
	}
	return record, nil
}

// Close closes the underlying file
func (r *Reader) Close() error {
	return r.file.Close()
}

// readHeader reads a record header from the given offset
func (r *Reader) readHeader(h hash.Hash32, offset int64) (*Header, error) {
	var headerBuf [recordHeaderSize]byte
	n, err := r.file.ReadAt(headerBuf[:], offset)
	if err != nil {
		return nil, err
	}
	if n != recordHeaderSize {
		return nil, fmt.Errorf("expected to read %d bytes, got %d", recordHeaderSize, n)
	}

	// Decode header data from the buffer
	header := &Header{}
	header.Timestamp = time.UnixMicro(int64(binary.LittleEndian.Uint64(headerBuf[0:])))
	header.KeySize = binary.LittleEndian.Uint32(headerBuf[8:])
	header.ValueSize = binary.LittleEndian.Uint32(headerBuf[12:])
	header.RecordType = headerBuf[16]
	header.ValueType = headerBuf[17]

	// Check if key / value size are within the set maximum values
	if header.KeySize > constants.MaxKeySize {
		return nil, ErrKeyTooLarge
	}
	if header.ValueSize > constants.MaxValueSize {
		return nil, ErrValueTooLarge
	}

	if h != nil {
		h.Write(headerBuf[:])
	}

	return header, nil
}
