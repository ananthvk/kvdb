package kvdb

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrNotExist    = errors.New("datastore does not exist")
)
