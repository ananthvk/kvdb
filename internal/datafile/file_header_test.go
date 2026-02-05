package datafile

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func TestWriteFileHeader(t *testing.T) {
	testFS := afero.NewMemMapFs()
	file, err := testFS.Create("0.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	ts := time.Now()
	header := NewFileHeader(ts, FILE_HEADER_SIZE)

	if err := WriteFileHeader(header, file); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	f, err := testFS.Open("0.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	fileContents, err := afero.ReadAll(f)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Check size
	if len(fileContents) != FILE_HEADER_SIZE {
		t.Errorf("expected header to be of size %d, got %d", FILE_HEADER_SIZE, len(fileContents))
	}

	// Check if the header was written correctly
	for i, b := range FILE_HEADER_MAGIC_BYTES {
		if fileContents[i] != b {
			t.Errorf("expected magic byte at index %d to be %d, got %d", i, b, fileContents[i])
		}
	}
}

func TestReadWriteFileHeader(t *testing.T) {
	testFS := afero.NewMemMapFs()
	file, err := testFS.Create("0.dat")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	ts := time.Now()
	header := NewFileHeader(ts, FILE_HEADER_SIZE)

	if err := WriteFileHeader(header, file); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	f, err := testFS.Open("0.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	readHeader, err := ReadFileHeader(f)
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	if readHeader.Timestamp.UnixMicro() != header.Timestamp.UnixMicro() {
		t.Errorf("expected timestamp %v, got %v", header.Timestamp, readHeader.Timestamp)
	}

	if readHeader.Offset != header.Offset {
		t.Errorf("expected offset %d, got %d", header.Offset, readHeader.Offset)
	}
}

func TestReadFileHeader_InvalidMagicBytes(t *testing.T) {
	testFS := afero.NewMemMapFs()
	file, err := testFS.Create("invalid_magic.dat")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	// Write invalid magic bytes
	invalidMagic := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if _, err := file.Write(invalidMagic); err != nil {
		t.Fatalf("failed to write invalid magic bytes: %v", err)
	}
	// Write 45 bytes
	file.Write([]byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9})
	file.Write([]byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9})
	file.Write([]byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9})
	file.Write([]byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9})
	file.Write([]byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9})
	file.Sync()

	// Attempt to read the header
	f, err := testFS.Open("invalid_magic.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	_, err = ReadFileHeader(f)
	if !errors.Is(err, ErrNotDataFile) {
		t.Fatalf("expected ErrNotDataFile error due to invalid magic bytes, got error %v", err)
	}
}
func TestReadFileHeader_IncompatibleVersion(t *testing.T) {
	testFS := afero.NewMemMapFs()
	file, err := testFS.Create("incompatible_version.dat")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	// Write valid magic bytes
	if _, err := file.Write(FILE_HEADER_MAGIC_BYTES[:]); err != nil {
		t.Fatalf("failed to write magic bytes: %v", err)
	}

	// Write an incompatible version
	incompatibleHeader := []byte{255, 0, 0} // Major version 255, minor and patch 0
	if _, err := file.Write(incompatibleHeader); err != nil {
		t.Fatalf("failed to write incompatible version: %v", err)
	}

	// Write timestamp and offset
	ts := time.Now().UnixMicro()
	if err := binary.Write(file, binary.LittleEndian, ts); err != nil {
		t.Fatalf("failed to write timestamp: %v", err)
	}
	offset := uint32(0)
	if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
		t.Fatalf("failed to write offset: %v", err)
	}
	// 1 Reserved byte
	file.Write([]byte{0x00})
	file.Sync()

	// Attempt to read the header
	f, err := testFS.Open("incompatible_version.dat")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	_, err = ReadFileHeader(f)
	if !errors.Is(err, ErrDataFileVersionNotCompatible) {
		t.Fatalf("expected ErrDataFileVersionNotCompatible error due to incompatible version, got error %v", err)
	}
}
