package record

import "errors"

var ErrCrcChecksumMismatch = errors.New("crc checksum does not match stored value")
