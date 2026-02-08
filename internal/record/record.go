package record

import "time"

const (
	recordHeaderSize = 20
	recordTypePut    = 0x50
	recordTypeDelete = 0x44
)

// Header contains metadata information about a log record
//
// Timestamp represents the time when the record was created or last modified.
// KeySize specifies the size in bytes of the record's key.
// ValueSize specifies the size in bytes of the record's value.
// RecordType indicates the type of operation (e.g., insert, update, delete).
// ValueType indicates the data type of the value (e.g., string, integer, blob). Currently it's set to 0x0
type Header struct {
	Timestamp  time.Time
	KeySize    uint32
	ValueSize  uint32
	RecordType uint8
	ValueType  uint8
}

// Record represents a single key-value pair in the log file. `Key` and `Value` can be empty depending upon the mode through which
// the record was read. Size represents the total size of the record (header + key + value + crc), it's useful for determining the start
// of the next record
type Record struct {
	Header Header
	Key    []byte
	Value  []byte
	Size   uint32
}

// newRecord returns a Record given the key, value and record type. The time of creation, key size and value size are set when this
// function is called
func newRecord(key []byte, value []byte, recordType uint8) *Record {
	return &Record{
		Header: Header{
			Timestamp:  time.Now(),
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(value)),
			RecordType: recordType,
			ValueType:  0x0,
		},
		Key:   key,
		Value: value,
		Size:  recordHeaderSize + uint32(len(key)) + uint32(len(value)) + 4, // 4 for the CRC32
	}
}
