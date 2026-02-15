package record

import (
	"fmt" // Added import for fmt
	"os"
	"testing"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/spf13/afero"
)

func TestNewScanner(t *testing.T) {
	fs := afero.NewMemMapFs()
	testFilePath := "/testfile"
	afero.WriteFile(fs, testFilePath, make([]byte, datafile.FileHeaderSize), os.ModePerm)

	scanner, err := NewScanner(fs, testFilePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer scanner.Close()

	if scanner.fs == nil {
		t.Fatal("expected fs to be initialized")
	}
	if scanner.file == nil {
		t.Fatal("expected file to be initialized")
	}
	if scanner.reader == nil {
		t.Fatal("expected reader to be initialized")
	}
}

func TestScanner_Scan(t *testing.T) {
	fs := afero.NewMemMapFs()
	keyValuePairs := []kv{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
	}
	testFilePath := createTestFile(t, fs, keyValuePairs)

	scanner, err := NewScanner(fs, testFilePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer scanner.Close()

	for _, kv := range keyValuePairs {
		record, _, err := scanner.Scan()
		if err != nil {
			t.Fatalf("expected no error on scan, got %v", err)
		}
		if string(record.Key) != string(kv.key) || string(record.Value) != string(kv.value) {
			t.Errorf("expected key %s and value %s, got key %s and value %s", kv.key, kv.value, record.Key, record.Value)
		}
	}
}

func TestScanner_Close(t *testing.T) {
	fs := afero.NewMemMapFs()
	testFilePath := "/testfile"
	afero.WriteFile(fs, testFilePath, make([]byte, datafile.FileHeaderSize), os.ModePerm)

	scanner, err := NewScanner(fs, testFilePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = scanner.Close()
	if err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

func createLargeTestFile(t *testing.T, fs afero.Fs, numRecords int) string {
	t.Helper()
	testFilePath := "/large_testfile"
	data := make([]byte, datafile.FileHeaderSize)
	file, err := fs.Create(testFilePath)
	if err != nil {
		t.Fatalf("expected no error writing header, got %v", err)
	}
	_, err = file.Write(data)
	if err != nil {
		t.Fatalf("expected no error writing header data, got %v", err)
	}

	writer, err := NewBufferedWriter(fs, testFilePath)
	if err != nil {
		t.Fatalf("expected no error creating writer, got %v", err)
	}
	defer writer.Close()

	for i := 0; i < numRecords; i++ {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		_, err := writer.WriteKeyValue(key, value)
		if err != nil {
			t.Fatalf("expected no error writing record, got %v", err)
		}
	}

	if err := writer.Sync(); err != nil {
		t.Fatalf("expected no error on sync, got %v", err)
	}
	writer.Close()

	return testFilePath
}

func TestScanner_Scan_Large(t *testing.T) {
	fs := afero.NewMemMapFs()
	numRecords := 1000
	testFilePath := createLargeTestFile(t, fs, numRecords)

	scanner, err := NewScanner(fs, testFilePath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer scanner.Close()

	for i := 0; i < numRecords; i++ {
		expectedKey := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)

		record, offset, err := scanner.Scan()
		if err != nil {
			t.Fatalf("expected no error on scan, got %v, offset %d", err, offset)
		}
		if string(record.Key) != expectedKey || string(record.Value) != expectedValue {
			t.Errorf("expected key %s and value %s, got key %s and value %s", expectedKey, expectedValue, record.Key, record.Value)
		}
	}

	// Ensure no more records are available
	_, _, err = scanner.Scan()
	if err == nil {
		t.Fatal("expected error on scanning past end of file, got none")
	}
}

func BenchmarkScanner_Scan(b *testing.B) {
	fs := afero.NewMemMapFs()
	numRecords := 1000
	testFilePath := createLargeTestFile(&testing.T{}, fs, numRecords)

	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		scanner, err := NewScanner(fs, testFilePath)
		b.StartTimer()
		if err != nil {
			b.Fatalf("expected no error, got %v", err)
		}

		for range numRecords {
			_, _, err := scanner.Scan()
			if err != nil {
				b.Fatalf("expected no error on scan, got %v", err)
			}
		}

		scanner.Close()
	}
}
