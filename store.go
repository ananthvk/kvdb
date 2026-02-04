package kvdb

import (
	"fmt"
	"unsafe"
)

// DataStore implements a Key-Value storage that can be used to store arbitrary keys and values
type DataStore struct {
	// Key-Value store implemented using Go standard map
	// []byte cannot be used as map key, but due to compiler optimization, a new string will not be initialized
	// https://go.dev/wiki/CompilerOptimizations#string-and-byte
	mp map[string][]byte
}

// NewDataStore creates a new datastore at the specified location, if a datastore already exists at the given location
// the existing datastore will be opened instead
func NewDataStore(directoryPath string) (*DataStore, error) {
	return &DataStore{
		mp: map[string][]byte{},
	}, nil
}

// Open opens the datastore at the specified location. If the datastore does not exist, an error is returned
func Open(directoryPath string) (*DataStore, error) {
	return &DataStore{
		mp: map[string][]byte{},
	}, nil
}

// Get returns the value associated with the key. If the key does not exist, `ErrNotFound` is returned, in case of any
// other errors, the error is returned
func (dataStore *DataStore) Get(key []byte) ([]byte, error) {
	val, ok := dataStore.mp[string(key)]
	if !ok {
		return nil, fmt.Errorf("%w - %q", ErrNotFound, key)
	}
	return val, nil
}

// Put sets the value for the specified key. It returns an error if the operation was not successful
func (dataStore *DataStore) Put(key []byte, value []byte) error {
	dataStore.mp[string(key)] = value
	return nil
}

// Delete deletes the value associated with the specified key. No error will be returned if the key does not exist.
// An error is returned if the deletion failed due to some other reason.
func (dataStore *DataStore) Delete(key []byte) error {
	delete(dataStore.mp, string(key))
	return nil
}

// ListKeys returns a list of all keys in the datastore. Note: This returns a slice of string, and is intended to be
// used for debug or inspection.
func (dataStore *DataStore) ListKeys() ([]string, error) {
	keys := make([]string, len(dataStore.mp))
	idx := 0
	for key := range dataStore.mp {
		keys[idx] = key
		idx++
	}
	return keys, nil
}

// Fold applies a function to each key-value pair in the datastore, accumulating a result starting from the initial accumulator value.
// NOTE: The function *MUST* not attempt to mutate the key slice, since it's an unsafe cast, and might lead to undefined behavior.
// Fold is implemented as a free function, because Go does not support generic methods. Generic methods are needed since `any` causes
// unnecessary memory allocations due to boxing/unboxing
func Fold[T any](dataStore *DataStore, fun func(key []byte, value []byte, acc0 T) T, acc0 T) (T, error) {
	for k, v := range dataStore.mp {
		view := unsafe.Slice(unsafe.StringData(k), len(k))
		acc0 = fun(view, v, acc0)
	}
	return acc0, nil
}

func (dataStore *DataStore) Merge(directoryPath string) error {
	// nop
	return nil
}

func (dataStore *DataStore) Sync() error {
	// nop
	return nil
}

// Size returns the number of keys present in the datastore
func (dataStore *DataStore) Size() int {
	return len(dataStore.mp)
}

// Close closes the datastore, writes pending changes (if any), and frees resources
func (dataStore *DataStore) Close() error {
	clear(dataStore.mp)
	return nil
}
