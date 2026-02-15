package metafile

import (
	"bufio"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

type MetaData struct {
	Type            string
	Version         string
	Created         string
	MaxDatafileSize int
}

const identifierFileName = "kvdb_store.meta"

// IsDatastore returns true if the given path points to a valid datastore.
// For a valid datastore, the path must point to a directory, and must exist, and
// a file named identiferFileName must be present at the path.
func IsDatastore(fs afero.Fs, path string) (bool, error) {
	// First check if the path exists
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	// Check if it's a directory
	isDir, err := afero.IsDir(fs, path)
	if err != nil {
		return false, err
	}
	if !isDir {
		return false, nil
	}

	// Check if the identifier file exists
	exists, err = afero.Exists(fs, filepath.Join(path, identifierFileName))
	if err != nil {
		return false, err
	}

	return exists, nil
}

// ReadMetaFile reads the metafile at the given path and returns the MetaData
func ReadMetaFile(fs afero.Fs, path string) (*MetaData, error) {
	file, err := fs.Open(filepath.Join(path, identifierFileName))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	metaData := MetaData{}
	empty := MetaData{}

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "type":
			metaData.Type = value
		case "version":
			metaData.Version = value
		case "created":
			metaData.Created = value
		case "max_datafile_size":
			fmt.Sscanf(value, "%d", &metaData.MaxDatafileSize)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if metaData == empty {
		return nil, errors.New("metafile is empty")
	}

	return &metaData, nil
}

// WriteMetaFile writes a meta file to the given directory, a file named identifierFileName will be written
func WriteMetaFile(fs afero.Fs, path string, metaData *MetaData) error {

	file, err := fs.Create(filepath.Join(path, identifierFileName))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	_, err = fmt.Fprintf(writer, "type=%s\n", metaData.Type)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "version=%s\n", metaData.Version)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "created=%s\n", metaData.Created)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "max_datafile_size=%d\n", metaData.MaxDatafileSize)
	if err != nil {
		return err
	}
	return nil
}

// IsValidPath returns true if the given directory path is valid for a
// new data store. For a path to be valid,
// 1) There should not be a file at the specified path
//
// 2) If the path is a directory, the directory has to be empty
//
// 3) There should not be an existing datastore at the given path
//
// 4) The path is also valid if there is nothing at the path (i.e. a new datastore can be constructed)
func IsValidPath(fs afero.Fs, path string) (bool, string, error) {
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return false, "", err
	}
	if !exists {
		return true, "", nil
	}

	isDir, err := afero.IsDir(fs, path)
	if err != nil {
		return false, "", err
	}
	if isDir {
		datastoreExists, err := IsDatastore(fs, path)
		if err != nil {
			return false, "", err
		}
		if datastoreExists {
			return false, "Datastore already exists at the path", nil
		}
		entries, err := afero.ReadDir(fs, path)
		if err != nil {
			return false, "", err
		}
		if len(entries) > 0 {
			return false, "Directory is not empty", nil
		}
	}

	return true, "", nil
}
