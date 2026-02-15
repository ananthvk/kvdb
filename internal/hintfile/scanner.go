package hintfile

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ananthvk/kvdb/internal/constants"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/spf13/afero"
)

const readerBufferSize = 4 * 1000 * 1000 // 4 MB

type Scanner struct {
	file         afero.File
	reader       *bufio.Reader
	sharedBuffer []byte // Buffer to hold hint record header + key
}

func NewScanner(fs afero.Fs, path string) (*Scanner, error) {
	file, err := fs.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(file, readerBufferSize)

	// Maximum size of a record (with a little bit extra for safety)
	const maxRecordSize = HintRecordHeaderSize + constants.MaxKeySize + 32

	return &Scanner{
		file:         file,
		reader:       reader,
		sharedBuffer: make([]byte, maxRecordSize),
	}, nil
}

// Returns the next hint record in the file
func (scanner *Scanner) Scan() (HintRecord, error) {
	n, err := io.ReadFull(scanner.reader, scanner.sharedBuffer[0:HintRecordHeaderSize])
	if err != nil {
		return HintRecord{}, err
	}
	if n != HintRecordHeaderSize {
		return HintRecord{}, fmt.Errorf("expected to read %d bytes, got %d", HintRecordHeaderSize, n)
	}

	// Process the hintRecord
	hintRecord := HintRecord{}
	hintRecord.Timestamp = time.UnixMicro(int64(binary.LittleEndian.Uint64(scanner.sharedBuffer[0:])))
	hintRecord.KeySize = binary.LittleEndian.Uint32(scanner.sharedBuffer[8:])
	hintRecord.ValueSize = binary.LittleEndian.Uint32(scanner.sharedBuffer[12:])
	hintRecord.ValuePos = int64(binary.LittleEndian.Uint64(scanner.sharedBuffer[16:]))

	// Check if key / value size are within the set maximum values
	// This is to detect corruption to header (i.e. if the size gets corrupted and it becomes a very huge value)
	if hintRecord.KeySize > constants.MaxKeySize {
		return HintRecord{}, record.ErrKeyTooLarge
	}
	if hintRecord.ValueSize > constants.MaxValueSize {
		return HintRecord{}, record.ErrValueTooLarge
	}

	keyStart := int(HintRecordHeaderSize)
	keyEnd := keyStart + int(hintRecord.KeySize)
	hintRecord.Key = scanner.sharedBuffer[keyStart:keyEnd]

	if _, err = io.ReadFull(scanner.reader, hintRecord.Key); err != nil {
		return HintRecord{}, err
	}

	return hintRecord, nil
}

func (scanner *Scanner) Close() error {
	return scanner.file.Close()
}
