package hintfile

import "time"

const HintRecordHeaderSize = 24 // 24 bytes

type HintRecord struct {
	Timestamp time.Time
	KeySize   uint32
	ValueSize uint32
	ValuePos  int64
	Key       []byte
}
