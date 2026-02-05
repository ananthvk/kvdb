package record

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"

	"github.com/spf13/afero"
)

const (
	recordHeaderSize = 20
	recordTypePut    = 0x50
	recordTypeDelete = 0x44
)

type record struct {
	Crc        uint32
	Timestamp  time.Time
	KeySize    uint32
	ValueSize  uint32
	RecordType uint8
	ValueType  uint8
	Key        []byte
	Value      []byte
}

func newRecord(key []byte, value []byte, recordType uint8) *record {
	return &record{
		Timestamp:  time.Now(),
		KeySize:    uint32(len(key)),
		ValueSize:  uint32(len(value)),
		RecordType: recordType,
		ValueType:  0x0,
		Key:        key,
		Value:      value,
	}
}

type Writer struct {
	fs   afero.Fs
	file afero.File
	// Internal buffer used to write record header
	buf [recordHeaderSize]byte
}

func NewWriter(fs afero.Fs, path string) (*Writer, error) {
	file, err := fs.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &Writer{
		fs:   fs,
		file: file,
	}, nil
}

func (w *Writer) writeRecord(r *record) error {
	h := crc32.NewIEEE()

	// Set header fields
	binary.LittleEndian.PutUint64(w.buf[0:], uint64(r.Timestamp.UnixMicro())) // Unix timestamp (in microseconds)
	binary.LittleEndian.PutUint32(w.buf[8:], r.KeySize)                       // Length of key
	binary.LittleEndian.PutUint32(w.buf[12:], r.ValueSize)                    // Length of value
	w.buf[16] = r.RecordType                                                  // Type of record, 0x50 for PUT, and 0x44 for DELETE
	w.buf[17] = r.ValueType                                                   // Currently value type is unused
	w.buf[18] = 0x0                                                           // Reserved
	w.buf[19] = 0x0                                                           // Reserved

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
	return nil
}

func (w *Writer) WriteKeyValue(key []byte, value []byte) error {
	rec := newRecord(key, value, recordTypePut)
	return w.writeRecord(rec)
}

func (w *Writer) WriteTombstone(key []byte) error {
	rec := newRecord(key, nil, recordTypeDelete)
	return w.writeRecord(rec)
}

func (w *Writer) Sync() error {
	return w.file.Sync()
}

func (w *Writer) Close() error {
	return w.file.Close()
}
