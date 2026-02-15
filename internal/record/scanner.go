package record

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/ananthvk/kvdb/internal/constants"
	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/spf13/afero"
)

const readerBufferSize = 4 * 1000 * 1000 // 4 MB

// Scanner sequentially reads records from the given file. It internally uses
// a buffered reader to improve performance. This is not meant to be used in Get operation, and is
// intended to be used for Merge (or other sequential scans of the datafile)
type Scanner struct {
	fs     afero.Fs
	file   afero.File
	offset int64
	reader *bufio.Reader

	headerBuf    [recordHeaderSize]byte
	crcHash      hash.Hash32
	sharedBuffer []byte
}

func NewScanner(fs afero.Fs, path string) (*Scanner, error) {
	file, err := fs.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(file, readerBufferSize)
	// Skip the file header
	_, err = reader.Discard(datafile.FileHeaderSize)
	if err != nil {
		return nil, err
	}

	// Max key size + Max value size + 4 bytes (padding) + a few bytes extra for safety
	const maxRecordSize = constants.MaxKeySize + constants.MaxValueSize + 128

	return &Scanner{
		fs:           fs,
		file:         file,
		reader:       reader,
		crcHash:      crc32.NewIEEE(),
		sharedBuffer: make([]byte, maxRecordSize),
	}, nil
}

// Scan returns the next record, the offset for the start of the record (from the first record)
// Note: They Key & Value inside record are backed by a shared buffer, and hence it'll be overwritten the next time
// Scan is called. If you need the record key / value later, make a copy
func (scanner *Scanner) Scan() (Record, int64, error) {
	scanner.crcHash.Reset()
	recordOffset := scanner.offset
	header, err := scanner.readHeader(scanner.crcHash)
	if err != nil {
		return Record{}, 0, err
	}

	keyStart := 0
	keyEnd := keyStart + int(header.KeySize)

	valStart := keyEnd
	valEnd := valStart + int(header.ValueSize)

	record := Record{
		Header: header,
		Key:    scanner.sharedBuffer[keyStart:keyEnd],
		Value:  scanner.sharedBuffer[valStart:valEnd],
		Size:   int64(recordHeaderSize + header.KeySize + header.ValueSize + 4),
	}

	if _, err = io.ReadFull(scanner.reader, record.Key); err != nil {
		return Record{}, 0, err
	}
	scanner.crcHash.Write(record.Key)
	if _, err = io.ReadFull(scanner.reader, record.Value); err != nil {
		return Record{}, 0, err
	}
	scanner.crcHash.Write(record.Value)

	// Check CRC
	crc := scanner.crcHash.Sum32()
	if _, err := io.ReadFull(scanner.reader, scanner.headerBuf[0:4]); err != nil {
		return Record{}, 0, err
	}
	fileCrc := binary.LittleEndian.Uint32(scanner.headerBuf[0:4])
	if fileCrc != crc {
		return Record{}, 0, ErrCrcChecksumMismatch
	}
	scanner.offset += record.Size
	return record, recordOffset, nil
}

// readHeader reads a record header at the current position
func (scanner *Scanner) readHeader(h hash.Hash32) (Header, error) {
	n, err := io.ReadFull(scanner.reader, scanner.headerBuf[:])
	if err != nil {
		return Header{}, err
	}
	if n != recordHeaderSize {
		return Header{}, fmt.Errorf("expected to read %d bytes, got %d", recordHeaderSize, n)
	}

	// Decode header data from the buffer
	header := Header{}
	header.Timestamp = time.UnixMicro(int64(binary.LittleEndian.Uint64(scanner.headerBuf[0:])))
	header.KeySize = binary.LittleEndian.Uint32(scanner.headerBuf[8:])
	header.ValueSize = binary.LittleEndian.Uint32(scanner.headerBuf[12:])
	header.RecordType = scanner.headerBuf[16]
	header.ValueType = scanner.headerBuf[17]

	// Check if key / value size are within the set maximum values
	// This is to detect corruption to header (i.e. if the size gets corrupted and it becomes a very huge value)
	if header.KeySize > constants.MaxKeySize {
		return Header{}, ErrKeyTooLarge
	}
	if header.ValueSize > constants.MaxValueSize {
		return Header{}, ErrValueTooLarge
	}

	if h != nil {
		h.Write(scanner.headerBuf[:])
	}

	return header, nil
}

func (scanner *Scanner) Close() error {
	return scanner.file.Close()
}
