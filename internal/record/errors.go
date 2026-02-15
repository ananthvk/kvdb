package record

import "errors"

var ErrCrcChecksumMismatch = errors.New("crc checksum does not match stored value")

var ErrKeyTooLarge = errors.New("key too large")

var ErrValueTooLarge = errors.New("value too large")
