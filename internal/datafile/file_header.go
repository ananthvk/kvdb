package datafile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/afero"
)

const fileHeaderVersionMajor = 2
const fileHeaderVersionMinor = 0
const fileHeaderVersionPatch = 0

var fileHeaderMagicBytes = [...]byte{0x00, 0x6B, 0x76, 0x64, 0x62, 0x44, 0x41, 0x54}

const FileHeaderSize = 19 // In bytes

var (
	ErrNotDataFile                  = errors.New("not a kvdb data file")
	ErrDataFileVersionNotCompatible = errors.New("datafile not supported by reader")
)

type FileHeader struct {
	VersionMajor byte
	VersionMinor byte
	VersionPatch byte
	Timestamp    time.Time
}

// NewFileHeader creates a new file header
func NewFileHeader(ts time.Time) *FileHeader {
	return &FileHeader{
		VersionMajor: fileHeaderVersionMajor,
		VersionMinor: fileHeaderVersionMinor,
		VersionPatch: fileHeaderVersionPatch,
		Timestamp:    ts,
	}
}

func isFileVersionCompatible(fileMajor, fileMinor, filePatch byte) error {
	// Major version mismatch - incompatible
	if fileMajor != fileHeaderVersionMajor {
		return fmt.Errorf(
			"%w - data file has major version %d, reader has major version %d",
			ErrDataFileVersionNotCompatible,
			fileMajor,
			fileHeaderVersionMajor,
		)
	}
	// File is newer (minor) than reader - incompatible
	if fileMinor > fileHeaderVersionMinor {
		return fmt.Errorf(
			"%w - file was created by newer version (%d.%d.%d) of the application",
			ErrDataFileVersionNotCompatible,
			fileMajor,
			fileMinor,
			filePatch,
		)
	}
	return nil
}

// ReadFileHeader reads a data file header from the file at the given path, it's assumed that the file pointer is at position 0, i.e. the beginning of the
// file before calling this function. This function returns an error if the file is not a data file, or if the file version is not
// compatible
func ReadFileHeader(fs afero.Fs, path string) (*FileHeader, error) {
	file, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf [FileHeaderSize]byte
	_, err = io.ReadFull(file, buf[:])
	if err != nil {
		return nil, err
	}
	// Check magic bytes to see if we are reading a data file
	for i, b := range fileHeaderMagicBytes {
		if buf[i] != b {
			return nil, ErrNotDataFile
		}
	}
	fileHeader := &FileHeader{
		VersionMajor: buf[8],
		VersionMinor: buf[9],
		VersionPatch: buf[10],
	}
	if err := isFileVersionCompatible(fileHeader.VersionMajor, fileHeader.VersionMinor, fileHeader.VersionPatch); err != nil {
		return nil, err
	}

	// Read timestamp
	ts := int64(binary.LittleEndian.Uint64(buf[11:]))
	fileHeader.Timestamp = time.UnixMicro(ts)

	return fileHeader, nil
}

// WriteFileHeader writes the data file header to the file at the given path. Note: It's assumed that the file pointer is at position 0 so that the header
// can be written first. It also calls `file.Sync()` after writing the header to ensure that the header was written completely.
// If the file already exists, it results in an error
func WriteFileHeader(fs afero.Fs, path string, ts time.Time) error {
	file, err := fs.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var buf [FileHeaderSize]byte

	// Copy magic bytes into buf
	copy(buf[:], fileHeaderMagicBytes[:])

	buf[8] = fileHeaderVersionMajor
	buf[9] = fileHeaderVersionMinor
	buf[10] = fileHeaderVersionPatch

	binary.LittleEndian.PutUint64(buf[11:], uint64(ts.UnixMicro()))

	if _, err := file.Write(buf[:]); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}
