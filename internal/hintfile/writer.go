package hintfile

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/ananthvk/kvdb/internal/constants"
	"github.com/ananthvk/kvdb/internal/record"
	"github.com/spf13/afero"
)

/*
Hints file are used to speed up startup, and are written during the merge & compaction process

In the bitcask paper, the following records are written: Tstamp, ksz, value_sz, value_pos, and key

When we are writing the merge files, i.e. `merge-1`, `merge-2` .... and so on, also write these fields into the hints file in `hints/` directory

Then, during the rename phase, when `merge-1` is renamed to `000....X.dat`, also rename `merge-1` in hints directory to `000...X.hint`

During startup, before loading a data file, check if a corresponding hint file exists in the `hints/` directory, if it exists, directly read from it to update keydir

Note: this implementation does not detect corruption due to disk issues, i.e. if the hints file gets corrupted due to the drive, or some other external program, it cannot detect it
*/

// Note: Hint file do not have any header, and are just raw records written to a file
// TODO: Later implement a header for hinit file too

const writerBufferSize = 4 * 1000 * 1000 // 4 MB

type Writer struct {
	file   afero.File
	writer *bufio.Writer
	buf    [HintRecordHeaderSize]byte
}

func NewWriter(fs afero.Fs, path string) (*Writer, error) {
	file, err := fs.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	// Write magic bytes

	return &Writer{
		file:   file,
		writer: bufio.NewWriterSize(file, writerBufferSize),
	}, nil
}

// WriteHintRecord writes the hint to the given file
func (w *Writer) WriteHintRecord(h *HintRecord) error {
	if int(h.KeySize) > constants.MaxKeySize {
		return record.ErrKeyTooLarge
	}
	if int(h.ValueSize) > constants.MaxValueSize {
		return record.ErrValueTooLarge
	}

	binary.LittleEndian.PutUint64(w.buf[0:], uint64(h.Timestamp.UnixMicro()))
	binary.LittleEndian.PutUint32(w.buf[8:], h.KeySize)
	binary.LittleEndian.PutUint32(w.buf[12:], h.ValueSize)
	binary.LittleEndian.PutUint64(w.buf[16:], uint64(h.ValuePos))

	// Write the hint header
	if _, err := w.writer.Write(w.buf[:]); err != nil {
		return err
	}

	// Write the hint value
	if _, err := w.writer.Write(h.Key); err != nil {
		return err
	}
	return nil
}

// Sync flushes any buffered data to the underlying file. It calls sync() on the file
func (w *Writer) Sync() error {
	w.writer.Flush()
	return w.file.Sync()
}

// Close closes the underlying file, it also writes any pending changes and syncs the changes to the disk
func (w *Writer) Close() error {
	w.writer.Flush()
	w.writer = nil
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
