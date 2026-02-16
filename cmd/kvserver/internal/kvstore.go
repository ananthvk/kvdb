package internal

// A wrapper around store, that also implements background compaction
// and periodic Sync

type KVStore struct {
}

func NewKVStore() *KVStore {
	return &KVStore{}
}
