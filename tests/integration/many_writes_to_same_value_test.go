package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ananthvk/kvdb"
	"github.com/ananthvk/kvdb/internal/metafile"
	"github.com/spf13/afero"
)

func TestManyWritesToSameValue(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "kvdb_many_writes_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Cleanup function to ensure directory is removed irrespective of test outcome
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to cleanup temp dir %s: %v", tempDir, err)
		}
	}()

	fs := afero.NewOsFs()
	dbPath := filepath.Join(tempDir, "test.db")

	// Step 1: Create the database
	store, err := kvdb.Create(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}

	// Step 2: Write 50 different keys
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("initial_key_%d", i))
		value := []byte(strconv.Itoa(i))
		if err := store.Put(key, value); err != nil {
			t.Fatalf("failed to put key %s: %v", key, err)
		}
	}

	// Step 3: Write the special key with initial counter value
	specialKey := []byte("thequickbrownfoxjumpsoverthelazydogs")
	counter := 0
	if err := store.Put(specialKey, []byte(strconv.Itoa(counter))); err != nil {
		t.Fatalf("failed to put special key: %v", err)
	}

	// Step 4: Write another 50 random values
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("random_key_%d", i))
		value := []byte(strconv.Itoa(i + 100))
		if err := store.Put(key, value); err != nil {
			t.Fatalf("failed to put random key %s: %v", key, err)
		}
	}

	// Step 5: Close the database
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	// Step 6: Edit the meta file to set low max data file size
	lowMaxDatafileSize := 100
	metaInfo, err := metafile.ReadMetaFile(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to read meta file: %v", err)
	}
	metaInfo.MaxDatafileSize = lowMaxDatafileSize
	if err := metafile.WriteMetaFile(fs, dbPath, metaInfo); err != nil {
		t.Fatalf("failed to write meta file: %v", err)
	}

	// Step 7: Reopen the database with low max data file size
	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}

	// Step 8: Write the same value 100k times, incrementing counter each time
	for i := 0; i < 100000; i++ {
		counter++
		if err := store.Put(specialKey, []byte(strconv.Itoa(counter))); err != nil {
			t.Fatalf("failed to put special key at iteration %d: %v", i, err)
		}
	}

	// Step 9: Close the database
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore after 100k writes: %v", err)
	}

	// Step 10: Reopen, verify count, then perform merge
	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore before merge: %v", err)
	}

	// Verify the counter value
	val, err := store.Get(specialKey)
	if err != nil {
		t.Fatalf("failed to get special key: %v", err)
	}
	retrievedCounter, err := strconv.Atoi(string(val))
	if err != nil {
		t.Fatalf("failed to parse counter value: %v", err)
	}
	if retrievedCounter != counter {
		t.Errorf("expected counter %d, got %d", counter, retrievedCounter)
	}

	// Perform merge
	if err := store.Merge(); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Step 11: Write another 20k-30k times (let's do 25k)
	for i := 0; i < 25000; i++ {
		counter++
		if err := store.Put(specialKey, []byte(strconv.Itoa(counter))); err != nil {
			t.Fatalf("failed to put special key at iteration %d (second batch): %v", i, err)
		}
	}

	// Step 12: Merge again
	if err := store.Merge(); err != nil {
		t.Fatalf("second merge failed: %v", err)
	}

	// Step 13: Close
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore after second merge: %v", err)
	}

	// Step 14: Open and verify counter
	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore for final verification: %v", err)
	}
	defer store.Close()

	val, err = store.Get(specialKey)
	if err != nil {
		t.Fatalf("failed to get special key for final verification: %v", err)
	}
	finalCounter, err := strconv.Atoi(string(val))
	if err != nil {
		t.Fatalf("failed to parse final counter value: %v", err)
	}
	expectedCounter := 100000 + 25000
	if finalCounter != expectedCounter {
		t.Errorf("expected final counter %d, got %d", expectedCounter, finalCounter)
	}

	t.Logf("Test completed successfully. Final counter value: %d", finalCounter)
}
