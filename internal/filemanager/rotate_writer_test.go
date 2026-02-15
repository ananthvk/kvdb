package filemanager

import (
	"testing"

	"github.com/spf13/afero"
)

func TestRotateWriter_Write(t *testing.T) {
	fs := afero.NewMemMapFs()
	fileCounter := 0
	getNextFilePath := func() string {
		fileCounter++
		return "testfile_" + string(rune(fileCounter)) + ".dat"
	}
	writer := NewRotateWriter(fs, 10, false, getNextFilePath)

	tests := []struct {
		key         []byte
		value       []byte
		isTombstone bool
		expectErr   bool
	}{
		{[]byte("key1"), []byte("value1"), false, false},
		{[]byte("key2"), []byte("value2"), false, false},
		{[]byte("key3"), []byte("value3"), true, false},
	}

	for _, tt := range tests {
		_, _, err := writer.Write(tt.key, tt.value, tt.isTombstone)
		if (err != nil) != tt.expectErr {
			t.Errorf("Write() error = %v, expectErr %v", err, tt.expectErr)
		}
	}

	// Test rotation
	writer.maxDatafileSize = 5
	_, _, err := writer.Write([]byte("key4"), []byte("value4"), false)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
	if !writer.shouldRotate {
		t.Error("Expected writer to rotate after exceeding maxDatafileSize")
	}
}

func TestRotateWriter_Close(t *testing.T) {
	fs := afero.NewMemMapFs()
	getNextFilePath := func() string {
		return "testfile.dat"
	}
	writer := NewRotateWriter(fs, 10, false, getNextFilePath)

	_, _, err := writer.Write([]byte("key1"), []byte("value1"), false)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestRotateWriter_Sync(t *testing.T) {
	fs := afero.NewMemMapFs()
	getNextFilePath := func() string {
		return "testfile.dat"
	}
	writer := NewRotateWriter(fs, 10, false, getNextFilePath)

	_, _, err := writer.Write([]byte("key1"), []byte("value1"), false)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	err = writer.Sync()
	if err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestRotateWriter_GetNewWriter_Error(t *testing.T) {
	fs := afero.NewMemMapFs()
	getNextFilePath := func() string {
		return "testfile.dat"
	}
	writer := NewRotateWriter(fs, 10, false, getNextFilePath)

	writer.getNextFilePath = func() string {
		return ""
	}

	err := writer.getNewWriter()
	if err == nil {
		t.Error("Expected error when getting new writer")
	}
}
func TestRotateWriter_MultipleRotations(t *testing.T) {
	fs := afero.NewMemMapFs()
	fileCounter := 0
	getNextFilePath := func() string {
		fileCounter++
		return "testfile_" + string(rune(48+fileCounter)) + ".dat"
	}
	writer := NewRotateWriter(fs, 20, false, getNextFilePath)

	// Write multiple records to trigger rotations
	for i := 0; i < 5; i++ {
		_, _, err := writer.Write([]byte("key"), []byte("value"), false)
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	if fileCounter < 2 {
		t.Errorf("Expected multiple file rotations, got %d files", fileCounter)
	}
}

func TestRotateWriter_TombstoneWriting(t *testing.T) {
	fs := afero.NewMemMapFs()
	writer := NewRotateWriter(fs, 100, false, func() string { return "testfile.dat" })

	filePath, _, err := writer.Write([]byte("key1"), []byte(""), true)
	if err != nil {
		t.Fatalf("WriteTombstone error = %v", err)
	}

	if filePath == "" {
		t.Error("Expected valid file path for tombstone write")
	}
}

func TestRotateWriter_SyncWithoutWriter(t *testing.T) {
	writer := NewRotateWriter(afero.NewMemMapFs(), 10, false, func() string { return "testfile.dat" })

	// Sync without any write should not error
	err := writer.Sync()
	if err != nil {
		t.Errorf("Sync() without writer error = %v", err)
	}
}

func TestRotateWriter_EmptyKeyValue(t *testing.T) {
	fs := afero.NewMemMapFs()
	writer := NewRotateWriter(fs, 100, false, func() string { return "testfile.dat" })

	_, _, err := writer.Write([]byte{}, []byte{}, false)
	if err != nil {
		t.Fatalf("Write() with empty key/value error = %v", err)
	}
}
