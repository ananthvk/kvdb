package record

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/spf13/afero"
)

// Reader is responsible for reading log records from a file. There are no locks in this implementation, so it's
// unsafe to call Reader methods concurrently
type Reader struct {
	fs         afero.Fs
	file       afero.File
	baseOffset uint32
	// Temporary fixed sized buffer to read the record header into
	buf [20]byte
}

// NewReader creates a new Record Reader that opens a file at the specified path for reading log records.
//
// baseOffset is the offset (in bytes) from the start of the file, that points to the first byte of the first log record. It is used
// to skip header fields and other metadata present at the start of the file.
func NewReader(fs afero.Fs, path string, baseOffset uint32) (*Reader, error) {
	file, err := fs.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &Reader{
		fs:         fs,
		file:       file,
		baseOffset: baseOffset,
	}, nil
}

// ReadValueAt reads a record at the given offset (from the start of the first record).
// It only reads and populates the value in the returned record. Key is left empty.
func (r *Reader) ReadValueAt(offset uint32) (*Record, error) {
	if _, err := r.file.Seek(int64(offset)+int64(r.baseOffset), io.SeekStart); err != nil {
		return nil, err
	}
	header, err := r.readHeader()
	if err != nil {
		return nil, err
	}
	record := &Record{
		Header: *header,
		Value:  make([]byte, header.ValueSize),
		Size:   recordHeaderSize + header.KeySize + header.ValueSize + 4,
	}
	// Seek to skip over the key
	if _, err := r.file.Seek(int64(header.KeySize), io.SeekCurrent); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r.file, record.Value); err != nil {
		return nil, err
	}
	return record, nil
}

// ReadKeyAt reads a record at the given offset (from the start of the first record).
// It only reads and populates the key in the returned record. Value is left empty.
func (r *Reader) ReadKeyAt(offset uint32) (*Record, error) {
	if _, err := r.file.Seek(int64(offset)+int64(r.baseOffset), io.SeekStart); err != nil {
		return nil, err
	}
	header, err := r.readHeader()
	if err != nil {
		return nil, err
	}
	record := &Record{
		Header: *header,
		Key:    make([]byte, header.KeySize),
		Size:   recordHeaderSize + header.KeySize + header.ValueSize + 4,
	}
	if _, err := io.ReadFull(r.file, record.Key); err != nil {
		return nil, err
	}
	return record, nil
}

// ReadRecordAt reads a record at the given offset (from the start of the first record).
// It reads both the key and value from the file, and both the Key and Value in the returned record are valid.
func (r *Reader) ReadRecordAt(offset uint32) (*Record, error) {
	if _, err := r.file.Seek(int64(offset)+int64(r.baseOffset), io.SeekStart); err != nil {
		return nil, err
	}
	header, err := r.readHeader()
	if err != nil {
		return nil, err
	}
	record := &Record{
		Header: *header,
		Key:    make([]byte, header.KeySize),
		Value:  make([]byte, header.ValueSize),
		Size:   recordHeaderSize + header.KeySize + header.ValueSize + 4,
	}

	if _, err := io.ReadFull(r.file, record.Key); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r.file, record.Value); err != nil {
		return nil, err
	}
	return record, nil
}

// ReadRecordAtStrict reads a record at the given offset (from the start of the first record).
// It reads both the key and value from the file, and both the Key and Value in the returned record are valid.
// It also verifies if the record is valid by computing the CRC checksum
func (r *Reader) ReadRecordAtStrict(offset uint32) (*Record, error) {
	if _, err := r.file.Seek(int64(offset)+int64(r.baseOffset), io.SeekStart); err != nil {
		return nil, err
	}

	h := crc32.NewIEEE()
	header, err := r.readHeader()
	if err != nil {
		return nil, err
	}

	// Update CRC with header info (that is present in r.buf)
	h.Write(r.buf[:])

	// TODO: Note introduce a check for max key / value size
	// since otherwise if the key / value size is corrupted, it may
	// cause a huge number of bytes to be allocated
	// Can also check file size to see if the file has that many bytes

	record := &Record{
		Header: *header,
		Key:    make([]byte, header.KeySize),
		Value:  make([]byte, header.ValueSize),
		Size:   recordHeaderSize + header.KeySize + header.ValueSize + 4,
	}

	if _, err := io.ReadFull(r.file, record.Key); err != nil {
		return nil, err
	}
	h.Write(record.Key)

	if _, err := io.ReadFull(r.file, record.Value); err != nil {
		return nil, err
	}
	h.Write(record.Value)

	crc := h.Sum32()

	// Read the checksum of the record from the file (4 bytes)
	// Reuse the same buffer
	if _, err := io.ReadFull(r.file, r.buf[0:4]); err != nil {
		return nil, err
	}

	fileCrc := binary.LittleEndian.Uint32(r.buf[0:4])

	if fileCrc != crc {
		return nil, ErrCrcChecksumMismatch
	}
	return record, nil
}

// Close closes the underlying file
func (r *Reader) Close() error {
	return r.file.Close()
}

// readHeader reads a record header from the current seek position in the file
func (r *Reader) readHeader() (*Header, error) {
	_, err := io.ReadFull(r.file, r.buf[:])
	if err != nil {
		return nil, err
	}

	// Decode header data from the buffer
	header := &Header{}
	header.Timestamp = time.UnixMicro(int64(binary.LittleEndian.Uint64(r.buf[0:])))
	header.KeySize = binary.LittleEndian.Uint32(r.buf[8:])
	header.ValueSize = binary.LittleEndian.Uint32(r.buf[12:])
	header.RecordType = r.buf[16]
	header.ValueType = r.buf[17]

	return header, nil
}
