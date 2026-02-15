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

// AddKeydirRecord adds a new KeydirRecord. If the timestamp is before the timestamp of an existing key, the update is ignored
func (k *Keydir) AddKeydirRecord(key []byte, fileId int, valueSize uint32, valuePos int64, timestamp time.Time) {
	// Ignore stale updates
	keyStr := string(key)
	if existing, ok := k.mp[keyStr]; ok {
		if timestamp.Before(existing.Timestamp) {
			return
		}
	}
	k.mp[keyStr] = KeydirRecord{
		FileId:    fileId,
		ValueSize: valueSize,
		ValuePos:  valuePos,
		Timestamp: timestamp,
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
