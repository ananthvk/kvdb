package kvdb

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")

	// TODO: For future
	ErrAlreadyExists = errors.New("datastore already exists")
	ErrNotExist      = errors.New("datastore does not exist")
	ErrNotADataStore = errors.New("path is not a datastore")
	ErrPathNotEmpty  = errors.New("path exists but is not a datastore")
)
