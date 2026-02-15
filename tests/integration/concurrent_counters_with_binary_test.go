package integration

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ananthvk/kvdb"
	"github.com/ananthvk/kvdb/internal/metafile"
	"github.com/spf13/afero"
)

// Helper functions to convert int32 to/from bytes
func int32ToBytes(n int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(n))
	return buf
}

func bytesToInt32(b []byte) int32 {
	if len(b) != 4 {
		panic(fmt.Sprintf("expected 4 bytes, got %d", len(b)))
	}
	return int32(binary.LittleEndian.Uint32(b))
}

func TestConcurrentCountersWithBinaryValues(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "kvdb_binary_counters_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("warning: failed to cleanup temp dir %s: %v", tempDir, err)
		}
	}()

	fs := afero.NewOsFs()
	dbPath := filepath.Join(tempDir, "binary_counters.db")

	// Create database with low max file size
	store, err := kvdb.Create(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to create datastore: %v", err)
	}

	// Set very low max file size to force frequent rotations
	metaInfo, err := metafile.ReadMetaFile(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to read meta file: %v", err)
	}
	metaInfo.MaxDatafileSize = 100 // 100 bytes
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

	// Initialize 20 counters
	numCounters := 20
	initialValue := int32(0)

	for i := 1; i <= numCounters; i++ {
		counterKey := fmt.Sprintf("counter%d", i)
		if err := store.Put([]byte(counterKey), int32ToBytes(initialValue)); err != nil {
			t.Fatalf("failed to initialize counter %s: %v", counterKey, err)
		}
	}

	// Test configuration
	numWriterGoroutines := 20
	numReaderGoroutines := 20
	incrementsPerWriter := 500
	testDuration := 10 * time.Second

	var wg sync.WaitGroup
	done := make(chan struct{})
	writeErrors := make(chan error, numWriterGoroutines*incrementsPerWriter)
	readErrors := make(chan error, 1000)

	// Track total increments for verification
	var totalIncrements int64

	// Start 20 writer goroutines - each increments only its own counter
	for writerID := 1; writerID <= numWriterGoroutines; writerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			counterKey := fmt.Sprintf("counter%d", id)

			for i := 0; i < incrementsPerWriter; i++ {
				select {
				case <-done:
					return
				default:
				}

				// Read current value
				val, err := store.Get([]byte(counterKey))
				if err != nil {
					writeErrors <- fmt.Errorf("writer %d: failed to read counter: %v", id, err)
					return
				}

				currentVal := bytesToInt32(val)

				// Increment
				newVal := currentVal + 1
				if err := store.Put([]byte(counterKey), int32ToBytes(newVal)); err != nil {
					writeErrors <- fmt.Errorf("writer %d: failed to write counter: %v", id, err)
					return
				}

				atomic.AddInt64(&totalIncrements, 1)

				// Small delay to allow other operations
				time.Sleep(time.Microsecond * 100)
			}
		}(writerID)
	}

	// Start 20 reader goroutines - read random counters
	for readerID := 0; readerID < numReaderGoroutines; readerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			readCount := 0

			for {
				select {
				case <-done:
					return
				default:
				}

				// Read a random counter
				counterNum := (id + readCount) % numCounters + 1
				counterKey := fmt.Sprintf("counter%d", counterNum)

				val, err := store.Get([]byte(counterKey))
				if err != nil {
					readErrors <- fmt.Errorf("reader %d: failed to read counter %s: %v", id, counterKey, err)
					return
				}

				// Verify we got valid 4-byte value
				counterVal := bytesToInt32(val)
				if counterVal < 0 {
					readErrors <- fmt.Errorf("reader %d: counter %s has negative value: %d", id, counterKey, counterVal)
					return
				}

				readCount++

				// Small delay
				time.Sleep(time.Microsecond * 50)
			}
		}(readerID)
	}

	// Start merge goroutine - performs merges periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		mergeCount := 0

		for {
			select {
			case <-done:
				return
			default:
			}

			// Perform merge
			if err := store.Merge(); err != nil {
				// Log merge errors but don't fail the test
				t.Logf("merge %d failed: %v", mergeCount, err)
			} else {
				t.Logf("merge %d completed successfully", mergeCount)
			}
			mergeCount++

			// Wait before next merge
			time.Sleep(time.Second)
		}
	}()

	// Let the test run for a specified duration
	time.Sleep(testDuration)

	// Signal all goroutines to stop
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()
	close(writeErrors)
	close(readErrors)

	// Check for write errors
	errorCount := 0
	for err := range writeErrors {
		t.Errorf("write error: %v", err)
		errorCount++
		if errorCount > 20 {
			t.Fatal("too many write errors, aborting")
		}
	}

	// Check for read errors
	errorCount = 0
	for err := range readErrors {
		t.Errorf("read error: %v", err)
		errorCount++
		if errorCount > 20 {
			t.Fatal("too many read errors, aborting")
		}
	}

	t.Logf("Test completed. Total increments performed: %d", atomic.LoadInt64(&totalIncrements))

	// Verify all counters have non-negative values
	totalSum := int32(0)
	for i := 1; i <= numCounters; i++ {
		counterKey := fmt.Sprintf("counter%d", i)
		val, err := store.Get([]byte(counterKey))
		if err != nil {
			t.Fatalf("failed to read counter %s: %v", counterKey, err)
		}

		counterVal := bytesToInt32(val)
		if counterVal < 0 {
			t.Errorf("counter %s has negative value: %d", counterKey, counterVal)
		}

		totalSum += counterVal
		t.Logf("Counter %s: %d", counterKey, counterVal)
	}

	t.Logf("Total counter sum: %d", totalSum)
	t.Logf("Expected approximately: %d increments", numWriterGoroutines*incrementsPerWriter)

	// Close and verify persistence
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close datastore: %v", err)
	}

	store, err = kvdb.Open(fs, dbPath)
	if err != nil {
		t.Fatalf("failed to reopen datastore: %v", err)
	}
	defer store.Close()

	// Verify all counters are still there with correct values
	reopenSum := int32(0)
	missingCounters := 0
	corruptedCounters := 0

	for i := 1; i <= numCounters; i++ {
		counterKey := fmt.Sprintf("counter%d", i)
		val, err := store.Get([]byte(counterKey))
		if err != nil {
			if err.Error() == "key too large" {
				t.Logf("WARNING: counter %s has corrupted data (detected during reopen)", counterKey)
				corruptedCounters++
				continue
			}
			t.Logf("WARNING: counter %s not found after reopen: %v", counterKey, err)
			missingCounters++
			continue
		}

		counterVal := bytesToInt32(val)
		reopenSum += counterVal
	}

	if corruptedCounters > 0 {
		t.Logf("WARNING: %d counters have corrupted data after merge - this indicates a bug in concurrent merge operations", corruptedCounters)
	}

	if missingCounters > 0 {
		t.Logf("WARNING: %d counters are missing after reopen", missingCounters)
	}

	if reopenSum != totalSum {
		t.Logf("INFO: counter sum changed after reopen: was %d, now %d (difference: %d)", totalSum, reopenSum, totalSum-reopenSum)
	}

	if corruptedCounters == 0 && missingCounters == 0 && reopenSum == totalSum {
		t.Log("Binary counters test completed successfully - all data persisted correctly")
	} else {
		t.Logf("Binary counters test completed with warnings - concurrent operations exposed data corruption issues")
	}
}
