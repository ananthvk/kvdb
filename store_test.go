package kvdb

import (
	"errors"
	"testing"
)

func TestStoreBasicTests(t *testing.T) {
	store, err := NewDataStore(":test")
	if err != nil {
		t.Fatalf("error occured while creating datastore")
	}

	// Test Put and Get
	key := []byte("testkey")
	value := []byte("testvalue")
	if err := store.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != string(value) {
		t.Errorf("expected %s, got %s", value, val)
	}

	// Test Get non-existent key
	_, err = store.Get([]byte("nonexistent"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Test Delete
	if err := store.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(key)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	// Test ListKeys
	store.Put([]byte("key1"), []byte("val1"))
	store.Put([]byte("key2"), []byte("val2"))
	keys, err := store.ListKeys()
	if err != nil || len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	// Test Fold
	acc, _ := Fold(store, func(k, v []byte, acc any) any {
		return acc.(int) + 1
	}, 0)
	if acc.(int) != 2 {
		t.Errorf("expected 2 items, got %d", acc.(int))
	}

	// Test Close
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestStoreMultiple(t *testing.T) {
	store, err := NewDataStore(":test")
	if err != nil {
		t.Fatalf("error occured while creating datastore")
	}
	defer store.Close()

	// Write initial values
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))

	// Read and verify
	val, _ := store.Get([]byte("key1"))
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", val)
	}

	// Update key1
	store.Put([]byte("key1"), []byte("updated1"))

	// Read updated value
	val, _ = store.Get([]byte("key1"))
	if string(val) != "updated1" {
		t.Errorf("expected updated1, got %s", val)
	}

	// Write more values
	store.Put([]byte("key3"), []byte("value3"))

	// Interleaved reads and writes
	val, _ = store.Get([]byte("key2"))
	store.Put([]byte("key2"), []byte("updated2"))
	val, _ = store.Get([]byte("key3"))
	store.Put([]byte("key1"), []byte("final1"))
	val, _ = store.Get([]byte("key1"))

	if string(val) != "final1" {
		t.Errorf("expected final1, got %s", val)
	}
}
