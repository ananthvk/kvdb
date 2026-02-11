package filemanager

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ananthvk/kvdb/internal/datafile"
	"github.com/spf13/afero"
)

func TestNewFileManager_EmptyDirectory(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm) // Create the data directory

	_, err := NewFileManager(fs, "", 1024)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Check if the file is named 0000000001.dat
	expectedFileName := "0000000001.dat"
	exists, err := afero.Exists(fs, "data/"+expectedFileName)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatalf("expected file %s to exist", expectedFileName)
	}
}

func TestNewFileManager_ExistingDataFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)                              // Create the data directory
	afero.WriteFile(fs, "data/0000000001.dat", []byte{}, 0755) // Create an existing data file

	_, err := NewFileManager(fs, "", 1024)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Check that no new data file is created
	exists, err := afero.Exists(fs, "data/0000000002.dat")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if exists {
		t.Fatalf("expected file 0000000002.dat to not exist")
	}
}

func TestFileManager_Write_SmallKeyValue(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	key := []byte("key1")
	value := []byte("val1")

	fileId, offset, err := manager.Write(key, value, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if fileId != 1 {
		t.Fatalf("expected fileId to be 1, got %d", fileId)
	}
	if offset < 0 {
		t.Fatalf("expected offset to be non-negative, got %d", offset)
	}
}

func TestFileManager_Write_ExceedingMaxSize(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Write small key-value pairs
	for i := range 5 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("val" + strconv.Itoa(i))
		_, _, err := manager.Write(key, value, false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	// Write a larger key-value pair that exceeds the max size
	largeKey := []byte("largeKey")
	largeValue := []byte(strings.Repeat("A", 60)) // 60 bytes
	fileId, _, err := manager.Write(largeKey, largeValue, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if fileId != 3 {
		t.Fatalf("expected new fileId to be 1, got %d", fileId)
	}

	// Verify that the new file is created
	expectedFileName := "0000000002.dat"
	exists, err := afero.Exists(fs, "data/"+expectedFileName)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatalf("expected file %s to exist", expectedFileName)
	}
}

func TestFileManager_Write_MultipleWrites(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Write multiple small key-value pairs
	for i := range 10 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("val" + strconv.Itoa(i))
		_, _, err := manager.Write(key, value, false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	// Check that a new file was created after exceeding the limit
	expectedFileName := "0000000002.dat"
	exists, err := afero.Exists(fs, "data/"+expectedFileName)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatalf("expected file %s to exist", expectedFileName)
	}
}

func TestFileManager_ReadRecordAtStrict(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Write a key-value pair
	key := []byte("key1")
	value := []byte("val1")
	_, _, err = manager.Write(key, value, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Read the record at the specific offset
	record, err := manager.ReadRecordAtStrict(1, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(record.Key) != string(key) {
		t.Fatalf("expected key %s, got %s", key, record.Key)
	}
	if string(record.Value) != string(value) {
		t.Fatalf("expected value %s, got %s", value, record.Value)
	}
}

func TestFileManager_ReadValueAt(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Write a key-value pair
	key := []byte("key1")
	value := []byte("val1")
	_, _, err = manager.Write(key, value, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Read the value at the specific offset
	record, err := manager.ReadValueAt(1, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(record.Value) != string(value) {
		t.Fatalf("expected value %s, got %s", value, record.Value)
	}
}

func TestFileManager_Write_MultipleFiles(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Write multiple small key-value pairs to exceed the max size and create multiple files
	for i := range 15 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("val" + strconv.Itoa(i))
		_, _, err := manager.Write(key, value, false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	// Check that multiple files were created
	expectedFileNames := []string{"0000000001.dat", "0000000002.dat"}
	for _, fileName := range expectedFileNames {
		exists, err := afero.Exists(fs, "data/"+fileName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !exists {
			t.Fatalf("expected file %s to exist", fileName)
		}
	}
}

func TestFileManager_Write_LargeNumberOfKeys(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("data", os.ModePerm)
	manager, err := NewFileManager(fs, "", 50)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var fileIds []int
	var offsets []int64

	// Write 1000 key-value pairs
	for i := range 1000 {
		key := []byte("key" + strconv.Itoa(i))
		value := []byte("val" + strconv.Itoa(i))
		fileId, offset, err := manager.Write(key, value, false)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		fileIds = append(fileIds, fileId)
		offsets = append(offsets, offset-datafile.FileHeaderSize)
	}

	// Read back the records to verify correctness
	for i := range 1000 {
		record, err := manager.ReadRecordAtStrict(fileIds[i], offsets[i])
		if err != nil {
			t.Fatalf("expected no error when reading record, got %v", err)
		}
		if string(record.Key) != "key"+strconv.Itoa(i) {
			t.Fatalf("expected key %s, got %s", "key"+strconv.Itoa(i), record.Key)
		}
		if string(record.Value) != "val"+strconv.Itoa(i) {
			t.Fatalf("expected value %s, got %s", "val"+strconv.Itoa(i), record.Value)
		}
	}
}
