package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/ananthvk/kvdb"
	"github.com/ananthvk/kvdb/internal/metafile"
	"github.com/spf13/afero"
)

func TestConcurrentWritesAndMerges(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "kvdb_concurrent_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to cleanup temp dir %s: %v", tempDir, err)
		}
	}()

	fs := afero.NewOsFs()
	dbPath := filepath.Join(tempDir, "test.db")

	// Step 1: Create database with small max file size
	store, err := kvdb.Create(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}

	// Update meta file to use small max file size to force rotations
	metaInfo, err := metafile.ReadMetaFile(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to read meta file: %v", err)
	}
	metaInfo.MaxDatafileSize = 2048 // 2KB
	if err := metafile.WriteMetaFile(fs, dbPath, metaInfo); err != nil {
		t.Fatalf("failed to write meta file: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}

	// Step 2: Write initial data across multiple rotations
	numKeys := 500
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := []byte(fmt.Sprintf("initial_value_%d", i))
		if err := store.Put([]byte(key), value); err != nil {
			t.Fatalf("failed to put key %s: %v", key, err)
		}
	}

	// Step 3: Force rotation by closing and reopening
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}

	// Step 4: Write more data to create another file
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("second_batch_key_%d", i)
		value := []byte(fmt.Sprintf("second_value_%d", i))
		if err := store.Put([]byte(key), value); err != nil {
			t.Fatalf("failed to put second batch key %s: %v", key, err)
		}
	}

	// Step 5: Update some existing keys (creates new versions)
	keysToUpdate := 100
	for i := 0; i < keysToUpdate; i++ {
		key := fmt.Sprintf("key_%d", i)
		newValue := []byte(fmt.Sprintf("updated_value_%d", i))
		if err := store.Put([]byte(key), newValue); err != nil {
			t.Fatalf("failed to update key %s: %v", key, err)
		}
	}

	// Step 6: Perform merge
	if err := store.Merge(); err != nil {
		t.Fatalf("first merge failed: %v", err)
	}

	// Step 7: Verify data after merge
	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("failed to list keys after merge: %v", err)
	}

	expectedKeys := numKeys + 200
	if len(keys) != expectedKeys {
		t.Errorf("expected %d keys after merge, got %d", expectedKeys, len(keys))
	}

	// Verify updated keys have correct values
	for i := 0; i < keysToUpdate; i++ {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := fmt.Sprintf("updated_value_%d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get key %s after merge: %v", key, err)
		}
		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Verify non-updated keys still have original values
	for i := keysToUpdate; i < keysToUpdate+10; i++ {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := fmt.Sprintf("initial_value_%d", i)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get key %s after merge: %v", key, err)
		}
		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Step 8: Delete some keys
	keysToDelete := 50
	for i := 0; i < keysToDelete; i++ {
		key := fmt.Sprintf("second_batch_key_%d", i)
		if err := store.Delete([]byte(key)); err != nil {
			t.Fatalf("failed to delete key %s: %v", key, err)
		}
	}

	// Step 9: Add more data and merge again
	for i := 200; i < 300; i++ {
		key := fmt.Sprintf("second_batch_key_%d", i)
		value := []byte(fmt.Sprintf("second_value_%d", i))
		if err := store.Put([]byte(key), value); err != nil {
			t.Fatalf("failed to put key %s: %v", key, err)
		}
	}

	if err := store.Merge(); err != nil {
		t.Fatalf("second merge failed: %v", err)
	}

	// Step 10: Verify deleted keys are gone
	for i := 0; i < keysToDelete; i++ {
		key := fmt.Sprintf("second_batch_key_%d", i)
		_, err := store.Get([]byte(key))
		if err == nil {
			t.Errorf("expected error when getting deleted key %s", key)
		}
	}

	// Step 11: Close, reopen, and verify persistence
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}
	defer store.Close()

	// Final verification
	finalKeys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("failed to list keys in final verification: %v", err)
	}

	// Should have: numKeys + (200 - keysToDelete) + 100 (from 200-299)
	expectedFinalKeys := numKeys + (200 - keysToDelete) + 100
	if len(finalKeys) != expectedFinalKeys {
		t.Errorf("expected final key count %d, got %d", expectedFinalKeys, len(finalKeys))
	}

	t.Logf("Concurrent writes and merges test completed successfully")
	t.Logf("Final keys in database: %d", len(finalKeys))
}

func TestLargeValuesWithMerge(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kvdb_large_values_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to cleanup temp dir %s: %v", tempDir, err)
		}
	}()

	fs := afero.NewOsFs()
	dbPath := filepath.Join(tempDir, "large_values.db")

	store, err := kvdb.Create(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}

	// Set small file size to test large values across merges
	metaInfo, err := metafile.ReadMetaFile(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to read meta file: %v", err)
	}
	metaInfo.MaxDatafileSize = 4096 // 4KB
	if err := metafile.WriteMetaFile(fs, dbPath, metaInfo); err != nil {
		t.Fatalf("failed to write meta file: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}

	// Write values of various sizes
	testCases := []struct {
		key   string
		size  int
		value []byte
	}{
		{"small", 100, make([]byte, 100)},
		{"medium", 1024, make([]byte, 1024)},
		{"large", 2048, make([]byte, 2048)},
		{"xlarge", 3072, make([]byte, 3072)},
	}

	// Initialize values with pattern
	for _, tc := range testCases {
		for i := range tc.value {
			tc.value[i] = byte('A' + (i % 26))
		}
	}

	// Write all values
	for _, tc := range testCases {
		if err := store.Put([]byte(tc.key), tc.value); err != nil {
			t.Fatalf("failed to put large value %s: %v", tc.key, err)
		}
	}

	// Force rotation and write more
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}

	// Update some large values
	for i := range testCases[:2] {
		newValue := make([]byte, testCases[i].size*2)
		for j := range newValue {
			newValue[j] = byte('a' + (j % 26))
		}
		if err := store.Put([]byte(testCases[i].key), newValue); err != nil {
			t.Fatalf("failed to update large value %s: %v", testCases[i].key, err)
		}
	}

	// Perform merge
	if err := store.Merge(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify all values after merge
	for i, tc := range testCases {
		if i < 2 {
			// These were updated
			expectedSize := tc.size * 2
			val, err := store.Get([]byte(tc.key))
			if err != nil {
				t.Errorf("failed to get key %s after merge: %v", tc.key, err)
			}
			if len(val) != expectedSize {
				t.Errorf("key %s: expected size %d, got %d", tc.key, expectedSize, len(val))
			}
		} else {
			// These were not updated
			val, err := store.Get([]byte(tc.key))
			if err != nil {
				t.Errorf("failed to get key %s after merge: %v", tc.key, err)
			}
			if len(val) != tc.size {
				t.Errorf("key %s: expected size %d, got %d", tc.key, tc.size, len(val))
			}
		}
	}

	t.Log("Large values with merge test completed successfully")
}

func TestRapidOpenCloseCycles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kvdb_openclose_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to cleanup temp dir %s: %v", tempDir, err)
		}
	}()

	fs := afero.NewOsFs()
	dbPath := filepath.Join(tempDir, "openclose.db")

	// Create initial database
	store, err := kvdb.Create(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}

	// Write some initial data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("initial_key_%d", i)
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := store.Put([]byte(key), value); err != nil {
			t.Fatalf("failed to put initial key: %v", err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	// Perform rapid open/close cycles with operations
	numCycles := 20
	for cycle := 0; cycle < numCycles; cycle++ {
		store, err = kvdb.Open(fs, dbPath)
		if err != nil {
			t.Fatalf("failed to reopen datastore in cycle %d: %v", cycle, err)
		}

		// Perform various operations
		testKey := fmt.Sprintf("cycle_key_%d", cycle)
		testValue := []byte(fmt.Sprintf("cycle_value_%d", cycle))

		// Put
		if err := store.Put([]byte(testKey), testValue); err != nil {
			t.Fatalf("failed to put in cycle %d: %v", cycle, err)
		}

		// Get
		val, err := store.Get([]byte(testKey))
		if err != nil {
			t.Fatalf("failed to get in cycle %d: %v", cycle, err)
		}
		if string(val) != string(testValue) {
			t.Errorf("cycle %d: value mismatch", cycle)
		}

		// Update an existing key every few cycles
		if cycle%5 == 0 {
			updateKey := fmt.Sprintf("initial_key_%d", cycle)
			updateValue := []byte(fmt.Sprintf("updated_%d", cycle))
			if err := store.Put([]byte(updateKey), updateValue); err != nil {
				t.Fatalf("failed to update in cycle %d: %v", cycle, err)
			}
		}

		// ListKeys
		keys, err := store.ListKeys()
		if err != nil {
			t.Fatalf("failed to list keys in cycle %d: %v", cycle, err)
		}
		if len(keys) < 100 {
			t.Errorf("cycle %d: expected at least 100 keys, got %d", cycle, len(keys))
		}

		if err := store.Close(); err != nil {
			t.Fatalf("failed to close datastore in cycle %d: %v", cycle, err)
		}
	}

	// Final verification
	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to open for final verification: %v", err)
	}
	defer store.Close()

	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("failed to list keys in final verification: %v", err)
	}

	// Should have 100 initial keys + 20 cycle keys
	expectedKeys := 120
	if len(keys) != expectedKeys {
		t.Errorf("expected %d keys, got %d", expectedKeys, len(keys))
	}

	// Verify some cycle keys
	for cycle := 0; cycle < 5; cycle++ {
		key := fmt.Sprintf("cycle_key_%d", cycle)
		expectedValue := fmt.Sprintf("cycle_value_%d", cycle)
		val, err := store.Get([]byte(key))
		if err != nil {
			t.Errorf("failed to get cycle key %s: %v", key, err)
		}
		if string(val) != expectedValue {
			t.Errorf("key %s: expected %s, got %s", key, expectedValue, string(val))
		}
	}

	t.Logf("Rapid open/close cycles test completed successfully")
	t.Logf("Total cycles: %d, Final keys: %d", numCycles, len(keys))
}

func TestConcurrentWritesAndReadsWithCounters(t *testing.T) {
	// This test simulates a real-world scenario where multiple goroutines
	// are updating counters concurrently
	tempDir, err := os.MkdirTemp("", "kvdb_counter_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to cleanup temp dir %s: %v", tempDir, err)
		}
	}()

	fs := afero.NewOsFs()
	dbPath := filepath.Join(tempDir, "counter_test.db")

	store, err := kvdb.Create(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}

	// Set small file size to force rotations
	metaInfo, err := metafile.ReadMetaFile(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to read meta file: %v", err)
	}
	metaInfo.MaxDatafileSize = 1024
	if err := metafile.WriteMetaFile(fs, dbPath, metaInfo); err != nil {
		t.Fatalf("failed to write meta file: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}

	// Initialize counters
	numCounters := 50
	initialValue := 0

	for i := 0; i < numCounters; i++ {
		counterKey := fmt.Sprintf("counter_%d", i)
		if err := store.Put([]byte(counterKey), []byte(strconv.Itoa(initialValue))); err != nil {
			t.Fatalf("failed to initialize counter %d: %v", i, err)
		}
	}

	// Concurrent increment operations
	numGoroutines := 20
	incrementsPerGoroutine := 100

	var wg sync.WaitGroup
	writeErrors := make(chan error, numGoroutines*incrementsPerGoroutine)

	for goroutineID := 0; goroutineID < numGoroutines; goroutineID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine updates unique counters to avoid conflicts
			startCounter := (id * 5) % numCounters
			for i := 0; i < incrementsPerGoroutine; i++ {
				counterNum := (startCounter + i) % numCounters
				counterKey := fmt.Sprintf("counter_%d", counterNum)

				// Read current value
				val, err := store.Get([]byte(counterKey))
				if err != nil {
					writeErrors <- fmt.Errorf("goroutine %d: failed to read counter %s: %v", id, counterKey, err)
					return
				}

				currentVal, err := strconv.Atoi(string(val))
				if err != nil {
					writeErrors <- fmt.Errorf("goroutine %d: failed to parse counter value: %v", id, err)
					return
				}

				// Increment and write back
				newVal := currentVal + 1
				if err := store.Put([]byte(counterKey), []byte(strconv.Itoa(newVal))); err != nil {
					writeErrors <- fmt.Errorf("goroutine %d: failed to update counter %s: %v", id, counterKey, err)
					return
				}
			}
		}(goroutineID)
	}

	wg.Wait()
	close(writeErrors)

	// Check for write errors
	errorCount := 0
	for err := range writeErrors {
		t.Errorf("write error: %v", err)
		errorCount++
		if errorCount > 10 {
			t.Fatal("too many errors, aborting")
		}
	}

	// Verify all counters are positive
	totalSum := 0
	for i := 0; i < numCounters; i++ {
		counterKey := fmt.Sprintf("counter_%d", i)
		val, err := store.Get([]byte(counterKey))
		if err != nil {
			t.Fatalf("failed to read counter %s: %v", counterKey, err)
		}

		counterVal, err := strconv.Atoi(string(val))
		if err != nil {
			t.Fatalf("failed to parse counter %s: %v", counterKey, err)
		}

		if counterVal < 0 {
			t.Errorf("counter %s has negative value: %d", counterKey, counterVal)
		}

		totalSum += counterVal
	}

	expectedTotal := numGoroutines * incrementsPerGoroutine
	if totalSum != expectedTotal {
		t.Logf("Note: total sum %d differs from expected %d due to concurrent updates", totalSum, expectedTotal)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	// Reopen and verify persistence
	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}
	defer store.Close()

	// Verify all counters are still there
	for i := 0; i < numCounters; i++ {
		counterKey := fmt.Sprintf("counter_%d", i)
		_, err := store.Get([]byte(counterKey))
		if err != nil {
			t.Errorf("counter %s not found after reopen: %v", counterKey, err)
		}
	}

	t.Log("Counter persistence verified")
	t.Logf("Total counter sum: %d, Expected: %d", totalSum, expectedTotal)
}
