package keydir

import "time"

type KeydirRecord struct {
	FileId    int
	ValueSize uint32
	// ValuePos is the offset to the start of the record (and not to the start of the value)
	ValuePos  int64
	Timestamp time.Time
}

type Keydir struct {
	mp map[string]KeydirRecord
}

// NewKeydir initializes a new Keydir
func NewKeydir() *Keydir {
	return &Keydir{
		mp: make(map[string]KeydirRecord),
	}
}

// AddKeydirRecord adds a new KeydirRecord
func (k *Keydir) AddKeydirRecord(key []byte, fileId int, valueSize uint32, valuePos int64, timestamp time.Time) {
	k.mp[string(key)] = KeydirRecord{
		FileId:    fileId,
		ValueSize: valueSize,
		ValuePos:  valuePos,
		Timestamp: timestamp,
	}
}

// UpdateKeydirRecord updates the fields of a KeydirRecord
func (k *Keydir) UpdateKeydirRecord(key []byte, valueSize uint32, valuePos int64, timestamp time.Time) {
	if record, exists := k.mp[string(key)]; exists {
		k.mp[string(key)] = KeydirRecord{
			FileId:    record.FileId,
			ValueSize: valueSize,
			ValuePos:  valuePos,
			Timestamp: timestamp,
		}
	}
}

// GetKeydirRecord retrieves a KeydirRecord by key
func (k *Keydir) GetKeydirRecord(key []byte) (KeydirRecord, bool) {
	record, exists := k.mp[string(key)]
	return record, exists
}

func (k *Keydir) DeleteRecord(key []byte) {
	delete(k.mp, string(key))
}

// GetAllKeys retrieves all keys in the Keydir as a slice
func (k *Keydir) GetAllKeys() []string {
	keys := make([]string, 0, len(k.mp))
	for key := range k.mp {
		keys = append(keys, key)
	}
	return keys
}

func (k *Keydir) Size() int {
	return len(k.mp)
}
