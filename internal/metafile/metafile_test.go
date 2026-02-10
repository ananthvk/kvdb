package metafile

import (
	"testing"

	"github.com/spf13/afero"
)

func TestIsDatastore(t *testing.T) {
	fs := afero.NewMemMapFs()

	// Test case 1: Path does not exist
	exists, err := IsDatastore(fs, "/nonexistent/path")
	if err != nil || exists {
		t.Errorf("Expected false, got %v, error: %v", exists, err)
	}

	// Test case 2: Path is not a directory
	afero.WriteFile(fs, "/nonexistent/path/file.txt", []byte("test"), 0644)
	exists, err = IsDatastore(fs, "/nonexistent/path/file.txt")
	if err != nil || exists {
		t.Errorf("Expected false, got %v, error: %v", exists, err)
	}

	// Test case 3: Path is a directory but identifier file is missing
	fs.MkdirAll("/datastore", 0755)
	exists, err = IsDatastore(fs, "/datastore")
	if err != nil || exists {
		t.Errorf("Expected false, got %v, error: %v", exists, err)
	}

	// Test case 4: Valid datastore
	afero.WriteFile(fs, "/datastore/kvdb_store.meta", []byte("type=example\nversion=1.0\ncreated=2023-01-01\nmax_key_size=1024\nmax_value_size=2048\nmax_datafile_size=1048576"), 0644)
	exists, err = IsDatastore(fs, "/datastore")
	if err != nil || !exists {
		t.Errorf("Expected true, got %v, error: %v", exists, err)
	}
}

func TestReadMetaFile(t *testing.T) {
	fs := afero.NewMemMapFs()

	// Test case 1: File does not exist
	metaData, err := ReadMetaFile(afero.Afero{Fs: fs}, "/nonexistent/path")
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	// Test case 2: File exists but is malformed
	afero.WriteFile(fs, "/datastore/kvdb_store.meta", []byte("malformed_line"), 0644)
	metaData, err = ReadMetaFile(afero.Afero{Fs: fs}, "/datastore")
	if err == nil {
		t.Errorf("Expected error, got nil: %v", err)
	}
	if metaData != nil {
		t.Errorf("Expected nil, got %v", metaData)
	}

	// Test case 3: Valid metadata file
	afero.WriteFile(fs, "/datastore/kvdb_store.meta", []byte("type=example\nversion=1.0\ncreated=2023-01-01\nmax_key_size=1024\nmax_value_size=2048\nmax_datafile_size=1048576"), 0644)
	metaData, err = ReadMetaFile(afero.Afero{Fs: fs}, "/datastore")
	if err != nil {
		t.Errorf("Expected nil, got error: %v", err)
	}
	if metaData.Type != "example" || metaData.Version != "1.0" || metaData.Created != "2023-01-01" || metaData.MaxKeySize != 1024 || metaData.MaxValueSize != 2048 {
		t.Errorf("Expected valid metadata, got %+v", metaData)
	}
}
func TestWriteMetaFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	metaData := &MetaData{
		Type:            "example",
		Version:         "1.0",
		Created:         "2023-01-01",
		MaxKeySize:      1024,
		MaxValueSize:    2048,
		MaxDatafileSize: 1048576,
	}

	// Test case 1: Write to a valid path
	err := WriteMetaFile(fs, "/datastore", metaData)
	if err != nil {
		t.Errorf("Expected nil, got error: %v", err)
	}

	// Verify the file was created and contains the correct data
	data, err := afero.ReadFile(fs, "/datastore/kvdb_store.meta")
	if err != nil {
		t.Errorf("Expected nil, got error: %v", err)
	}
	expected := "type=example\nversion=1.0\ncreated=2023-01-01\nmax_key_size=1024\nmax_value_size=2048\nmax_datafile_size=1048576\n"
	if string(data) != expected {
		t.Errorf("Expected data:\n%s\nGot:\n%s", expected, string(data))
	}

}
