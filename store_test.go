package kvdb

import (
	"errors"
	"testing"

	"github.com/spf13/afero"
)

// TODO: Add tests for opening existing db

func helperCreateMultipleDataFiles(t *testing.T, fs afero.Fs, path string) *DataStore {
	t.Helper()
	store, err := Create(fs, path)
	if err != nil {
		t.Fatalf("error creating datastore: %v", err)
	}
	return store
}

func TestMergeBasic(t *testing.T) {
	fs := afero.NewMemMapFs()
	store := helperCreateMultipleDataFiles(t, fs, "test_merge_basic.db")
	defer store.Close()

	// File 1: Initial set of keys
	store.Put([]byte("key1"), []byte("value1_file1"))
	store.Put([]byte("key2"), []byte("value2_file1"))
	store.Put([]byte("key3"), []byte("value3_file1"))

	// Force rotation by closing and reopening (creates new file on next write)
	store.Close()
	store, err := Open(fs, "test_merge_basic.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 2: Update some keys, add new ones
	store.Put([]byte("key1"), []byte("value1_file2")) // Updated in file2
	store.Put([]byte("key4"), []byte("value4_file2"))

	// Force another rotation
	store.Close()
	store, err = Open(fs, "test_merge_basic.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 3: More updates and new keys
	store.Put([]byte("key2"), []byte("value2_file3")) // Updated in file3
	store.Put([]byte("key5"), []byte("value5_file3"))

	// Verify initial state
	keys, _ := store.ListKeys()
	if len(keys) != 5 {
		t.Errorf("expected 5 keys before merge, got %d", len(keys))
	}

	// Run merge
	if err := store.Merge(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify all keys are still accessible
	val, err := store.Get([]byte("key1"))
	if err != nil {
		t.Errorf("key1 not found after merge: %v", err)
	} else if string(val) != "value1_file2" {
		t.Errorf("key1: expected value1_file2, got %s", string(val))
	}

	val, err = store.Get([]byte("key2"))
	if err != nil {
		t.Errorf("key2 not found after merge: %v", err)
	} else if string(val) != "value2_file3" {
		t.Errorf("key2: expected value2_file3, got %s", string(val))
	}

	val, err = store.Get([]byte("key3"))
	if err != nil {
		t.Errorf("key3 not found after merge: %v", err)
	} else if string(val) != "value3_file1" {
		t.Errorf("key3: expected value3_file1, got %s", string(val))
	}

	val, err = store.Get([]byte("key4"))
	if err != nil {
		t.Errorf("key4 not found after merge: %v", err)
	} else if string(val) != "value4_file2" {
		t.Errorf("key4: expected value4_file2, got %s", string(val))
	}

	val, err = store.Get([]byte("key5"))
	if err != nil {
		t.Errorf("key5 not found after merge: %v", err)
	} else if string(val) != "value5_file3" {
		t.Errorf("key5: expected value5_file3, got %s", string(val))
	}
}

func TestMergeWithDeletedKeys(t *testing.T) {
	fs := afero.NewMemMapFs()
	store := helperCreateMultipleDataFiles(t, fs, "test_merge_delete.db")
	defer store.Close()

	// File 1: Add keys
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))
	store.Put([]byte("key3"), []byte("value3"))

	// Force rotation
	store.Close()
	store, err := Open(fs, "test_merge_delete.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 2: Delete some keys
	store.Delete([]byte("key1")) // Tombstone in file2
	store.Put([]byte("key4"), []byte("value4"))

	// Force another rotation
	store.Close()
	store, err = Open(fs, "test_merge_delete.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 3: Delete another key
	store.Delete([]byte("key2")) // Tombstone in file3

	// Verify state before merge
	keys, _ := store.ListKeys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys before merge (key3, key4), got %d", len(keys))
	}

	// Run merge
	if err := store.Merge(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify deleted keys are still not found
	_, err = store.Get([]byte("key1"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("key1: expected ErrKeyNotFound, got %v", err)
	}

	_, err = store.Get([]byte("key2"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("key2: expected ErrKeyNotFound, got %v", err)
	}

	// Verify remaining keys are accessible
	val, err := store.Get([]byte("key3"))
	if err != nil {
		t.Errorf("key3 not found after merge: %v", err)
	} else if string(val) != "value3" {
		t.Errorf("key3: expected value3, got %s", string(val))
	}

	val, err = store.Get([]byte("key4"))
	if err != nil {
		t.Errorf("key4 not found after merge: %v", err)
	} else if string(val) != "value4" {
		t.Errorf("key4: expected value4, got %s", string(val))
	}

	// Verify key count
	keys, _ = store.ListKeys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys after merge, got %d", len(keys))
	}
}

func TestMergeWithNoImmutableFiles(t *testing.T) {
	fs := afero.NewMemMapFs()
	store := helperCreateMultipleDataFiles(t, fs, "test_merge_no_immutable.db")
	defer store.Close()

	// Add some data to the active file
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))

	// Merge with no immutable files should succeed
	if err := store.Merge(); err != nil {
		t.Fatalf("merge failed with no immutable files: %v", err)
	}

	// Verify data is still accessible
	val, err := store.Get([]byte("key1"))
	if err != nil {
		t.Errorf("key1 not found after merge: %v", err)
	} else if string(val) != "value1" {
		t.Errorf("key1: expected value1, got %s", string(val))
	}
}

func TestMergeDataIntegrity(t *testing.T) {
	fs := afero.NewMemMapFs()
	store := helperCreateMultipleDataFiles(t, fs, "test_merge_integrity.db")
	defer store.Close()

	// Create a comprehensive dataset across multiple files
	testData := map[string]string{
		"a": "alpha",
		"b": "bravo",
		"c": "charlie",
		"d": "delta",
		"e": "echo",
		"f": "foxtrot",
		"g": "golf",
		"h": "hotel",
		"i": "india",
		"j": "juliet",
	}

	// File 1: Add all keys
	for k, v := range testData {
		store.Put([]byte(k), []byte(v))
	}

	// Force rotation
	store.Close()
	store, err := Open(fs, "test_merge_integrity.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 2: Update some keys, delete others
	store.Put([]byte("a"), []byte("alpha_updated"))
	store.Put([]byte("b"), []byte("bravo_updated"))
	store.Delete([]byte("c"))

	// Force rotation
	store.Close()
	store, err = Open(fs, "test_merge_integrity.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 3: More updates
	store.Put([]byte("d"), []byte("delta_updated"))
	store.Put([]byte("e"), []byte("echo_updated"))
	store.Delete([]byte("f"))

	// Get expected state before merge
	expectedState := map[string]string{}
	for k := range testData {
		val, err := store.Get([]byte(k))
		if err == nil {
			expectedState[k] = string(val)
		}
	}

	// Run merge
	if err := store.Merge(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify all expected keys are accessible with correct values
	for k, expectedVal := range expectedState {
		val, err := store.Get([]byte(k))
		if err != nil {
			t.Errorf("key %s not found after merge: %v", k, err)
		} else if string(val) != expectedVal {
			t.Errorf("key %s: expected %s, got %s", k, expectedVal, string(val))
		}
	}

	// Verify deleted keys are still deleted
	for _, k := range []string{"c", "f"} {
		_, err := store.Get([]byte(k))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("key %s: expected ErrKeyNotFound, got %v", k, err)
		}
	}

	// Verify total key count
	keys, _ := store.ListKeys()
	if len(keys) != len(expectedState) {
		t.Errorf("expected %d keys after merge, got %d", len(expectedState), len(keys))
	}
}

func TestMergeMultipleTimes(t *testing.T) {
	fs := afero.NewMemMapFs()
	store := helperCreateMultipleDataFiles(t, fs, "test_merge_multiple.db")
	defer store.Close()

	// File 1: Add keys
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))

	// Force rotation
	store.Close()
	store, err := Open(fs, "test_merge_multiple.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 2: Add more keys
	store.Put([]byte("key3"), []byte("value3"))
	store.Put([]byte("key4"), []byte("value4"))

	// First merge
	if err := store.Merge(); err != nil {
		t.Fatalf("first merge failed: %v", err)
	}

	// Verify first merge
	val, _ := store.Get([]byte("key1"))
	if string(val) != "value1" {
		t.Errorf("key1: expected value1, got %s", string(val))
	}

	// Add more data to create new immutable files
	store.Close()
	store, err = Open(fs, "test_merge_multiple.db")
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	// File 3: Add more keys
	store.Put([]byte("key5"), []byte("value5"))

	// Second merge should also succeed
	if err := store.Merge(); err != nil {
		t.Fatalf("second merge failed: %v", err)
	}

	// Verify all keys are still accessible
	keys, _ := store.ListKeys()
	if len(keys) != 5 {
		t.Errorf("expected 5 keys after second merge, got %d", len(keys))
	}

	val, _ = store.Get([]byte("key5"))
	if string(val) != "value5" {
		t.Errorf("key5: expected value5, got %s", string(val))
	}
}

func TestStoreBasicTests(t *testing.T) {
	fs := afero.NewMemMapFs()
	store, err := Create(fs, "0.dat")
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
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Test Delete
	if err := store.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}

	// Test ListKeys
	store.Put([]byte("key1"), []byte("val1"))
	store.Put([]byte("key2"), []byte("val2"))
	keys, err := store.ListKeys()
	if err != nil || len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	// Test Close
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestStoreMultiple(t *testing.T) {
	fs := afero.NewMemMapFs()
	store, err := Create(fs, "0.dat")
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
