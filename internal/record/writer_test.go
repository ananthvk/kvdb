package record

import (
	"io"
	"testing"

	"github.com/spf13/afero"
)

func TestWriteKeyValue(t *testing.T) {
	testFS := afero.NewMemMapFs()
	datafile, err := NewWriter(testFS, "0.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	datafile.WriteKeyValue([]byte("123"), []byte("abcd"))
	datafile.Close()

	file, err := testFS.Open("0.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// 20 for the record header, 4 for the CRC, 7 for the data
	const expectedLength = 20 + 4 + 7
	if len(data) != expectedLength {
		t.Errorf("expected data length of %d, got %d", expectedLength, len(data))
	}
}

func TestWriteMultiple(t *testing.T) {
	testFS := afero.NewMemMapFs()
	datafile, err := NewWriter(testFS, "1.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	for range 100 {
		datafile.WriteKeyValue([]byte("123"), []byte("abcd"))
	}
	datafile.Close()

	file, err := testFS.Open("1.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// (20 for the record header, 4 for the CRC, 7 for the data) * 100, since 100 records
	const expectedLength = (20 + 4 + 7) * 100
	if len(data) != expectedLength {
		t.Errorf("expected data length of %d, got %d", expectedLength, len(data))
	}
}

func TestWriteTombstone(t *testing.T) {
	testFS := afero.NewMemMapFs()
	datafile, err := NewWriter(testFS, "2.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	datafile.WriteTombstone([]byte("123"))
	datafile.Close()

	file, err := testFS.Open("2.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// 20 for the record header, 4 for the CRC, 3 for the key (no value)
	const expectedLength = 20 + 4 + 3
	if len(data) != expectedLength {
		t.Errorf("expected data length of %d, got %d", expectedLength, len(data))
	}
}
