package record

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/spf13/afero"
)

type kv struct {
	key   []byte
	value []byte
}

type readerFn func(offset uint32) (*Record, error)

func createTestFile(t *testing.T, fs afero.Fs, initialFileData []byte, keyValuePairs []kv) string {
	t.Helper()
	fileName := uuid.NewString()
	f, err := fs.Create(fileName)
	if err != nil {
		t.Fatalf("could not open file %s", fileName)
	}
	if _, err := f.Write(initialFileData); err != nil {
		t.Fatalf("could not write initial data to test file %s", fileName)
	}
	f.Close()

	writer, err := NewWriter(fs, fileName)
	if err != nil {
		t.Fatalf("could not create writer %s", fileName)
	}
	defer writer.Close()

	for _, kv := range keyValuePairs {
		if err := writer.WriteKeyValue(kv.key, kv.value); err != nil {
			t.Fatalf("could not write record %v", kv)
		}
	}

	return fileName
}

var testData = []kv{
	{key: []byte("xyz123"), value: []byte("some value stored")},
	{key: []byte("alice"), value: []byte(`{"username": "al12", "email": "alice@example.com", "age": 30}`)},
	{key: []byte("bob"), value: []byte(`{"username": "bob42", "email": "bob@example.com", "phone": "123-456-7890"}`)},
	{key: []byte("charlie"), value: []byte(`{"username": "ch99", "email": "charlie@example.com"}`)},
	{key: []byte("diana"), value: []byte(`{"username": "diana", "email": "diana@example.com", "address": "123 Main St"}`)},
	{key: []byte("eve"), value: []byte(`{"username": "eve7", "email": "eve@example.com"}`)},
	{key: []byte("frank"), value: []byte(`{"username": "frank", "email": "frank@example.com", "age": 28, "phone": "987-654-3210"}`)},
	{key: []byte("grace"), value: []byte(`{"username": "gra33", "email": "grace@example.com"}`)},
	{key: []byte("henry"), value: []byte(`{"username": "hen88", "email": "henry@example.com", "address": "456 Elm St"}`)},
	{key: []byte("iris"), value: []byte(`{"username": "iris", "email": "iris@example.com"}`)},
	{key: []byte("hh"), value: []byte(``)},
	{key: []byte("david"), value: []byte(`{"username": "hen88", "email": "henry@example.com", "address": "456 Elm St"}`)},
	{key: []byte(""), value: []byte("empty key")},
	{key: []byte("no value"), value: []byte("")},
	{key: []byte(""), value: []byte("")},
	{key: []byte("a"), value: []byte("b")},
	{key: []byte(string(make([]byte, 1000))), value: []byte("large key")},
	{key: []byte("alice"), value: []byte(`new value`)},
}

func TestReaderEmptyFile(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("")
	fileName := createTestFile(t, testFS, initialData, nil)
	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader %s", err)
	}
	if _, err := reader.ReadRecordAtStrict(0); err == nil {
		t.Errorf("expected error reading from empty file, got nil")
	}
	reader.Close()

	// Same test, but with random bytes at the start of the file (to simulate file header, but no content)
	initialData = []byte("xthisisaheaderthatisatthestartofthefile")
	fileName = createTestFile(t, testFS, initialData, nil)
	reader, err = NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader %s", err)
	}
	defer reader.Close()
	fns := []readerFn{reader.ReadRecordAtStrict, reader.ReadRecordAt, reader.ReadKeyAt, reader.ReadValueAt}
	for _, fn := range fns {
		if _, err := fn(0); err == nil {
			t.Errorf("expected error reading from empty file, got nil")
		}
	}
}

func TestReaderSingleRecord(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")
	fileName := createTestFile(t, testFS, initialData,
		[]kv{
			{
				key:   []byte("hello world"),
				value: []byte("the quick brown fox jumps over the lazy dogs"),
			},
		},
	)
	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader %s", err)
	}
	defer reader.Close()
	fns := []readerFn{reader.ReadRecordAtStrict, reader.ReadRecordAt}
	for _, fn := range fns {
		record, err := fn(0)
		if err != nil {
			t.Fatalf("error reading record: %v", err)
		}

		if string(record.Key) != "hello world" {
			t.Errorf("expected key 'hello world', got %s", string(record.Key))
		}
		if string(record.Value) != "the quick brown fox jumps over the lazy dogs" {
			t.Errorf("expected value 'the quick brown fox jumps over the lazy dogs', got %s", string(record.Value))
		}
	}

}

func TestReaderMultipleRecords(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")
	fileName := createTestFile(t, testFS, initialData, testData)
	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader %s", err)
	}
	defer reader.Close()

	fns := []readerFn{reader.ReadRecordAtStrict, reader.ReadRecordAt}
	for _, fn := range fns {
		var offset uint32 = 0
		for i, expected := range testData {
			record, err := fn(offset)
			if err != nil {
				t.Fatalf("error reading record %d: %v", i, err)
			}
			if string(record.Key) != string(expected.key) {
				t.Errorf("record %d: expected key %s, got %s", i, string(expected.key), string(record.Key))
			}
			if string(record.Value) != string(expected.value) {
				t.Errorf("record %d: expected value %s, got %s", i, string(expected.value), string(record.Value))
			}
			offset += record.Size
		}
	}
}

func TestReaderKeyAndValueMethods(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")
	fileName := createTestFile(t, testFS, initialData, testData)
	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader %s", err)
	}
	defer reader.Close()

	var offset uint32 = 0
	for i, expected := range testData {
		// Test ReadKeyAt
		keyRecord, err := reader.ReadKeyAt(offset)
		if err != nil {
			t.Fatalf("error reading key at record %d: %v", i, err)
		}
		if string(keyRecord.Key) != string(expected.key) {
			t.Errorf("record %d (ReadKeyAt): expected key %s, got %s", i, string(expected.key), string(keyRecord.Key))
		}
		if len(keyRecord.Value) != 0 {
			t.Errorf("record %d (ReadKeyAt): expected empty value, got %s", i, string(keyRecord.Value))
		}

		// Test ReadValueAt
		valueRecord, err := reader.ReadValueAt(offset)
		if err != nil {
			t.Fatalf("error reading value at record %d: %v", i, err)
		}
		if string(valueRecord.Value) != string(expected.value) {
			t.Errorf("record %d (ReadValueAt): expected value %s, got %s", i, string(expected.value), string(valueRecord.Value))
		}
		if len(valueRecord.Key) != 0 {
			t.Errorf("record %d (ReadValueAt): expected empty key, got %s", i, string(valueRecord.Key))
		}

		// Verify header and size are populated in both cases
		if keyRecord.Header.KeySize != valueRecord.Header.KeySize {
			t.Errorf("record %d: key size mismatch between ReadKeyAt and ReadValueAt", i)
		}
		if keyRecord.Header.ValueSize != valueRecord.Header.ValueSize {
			t.Errorf("record %d: value size mismatch between ReadKeyAt and ReadValueAt", i)
		}

		offset += keyRecord.Size
	}
}

func TestReaderCorruptedData(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")
	fileName := createTestFile(t, testFS, initialData, testData)

	// Read the file and corrupt some bytes
	f, err := testFS.OpenFile(fileName, 0x0002, 0666) // os.O_WRONLY
	if err != nil {
		t.Fatalf("could not open file for corruption: %v", err)
	}
	defer f.Close()

	// Corrupt the checksum of the first record (last 4 bytes)
	// First record header is 20 bytes, then key and value, then 4-byte checksum
	firstRecordSize := uint32(20) + uint32(len(testData[0].key)) + uint32(len(testData[0].value))
	if _, err := f.Seek(int64(len(initialData))+int64(firstRecordSize), 0); err != nil {
		t.Fatalf("could not seek to checksum position: %v", err)
	}
	if _, err := f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}); err != nil {
		t.Fatalf("could not write corrupted checksum: %v", err)
	}
	f.Close()

	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader: %v", err)
	}
	defer reader.Close()

	// ReadRecordAtStrict should fail due to CRC mismatch
	if _, err := reader.ReadRecordAtStrict(0); err != ErrCrcChecksumMismatch {
		t.Errorf("expected ErrCrcChecksumMismatch, got %v", err)
	}

	// ReadRecordAt should still work (no checksum verification)
	record, err := reader.ReadRecordAt(0)
	if err != nil {
		t.Errorf("ReadRecordAt should succeed despite corruption: %v", err)
	}
	if string(record.Key) != string(testData[0].key) {
		t.Errorf("expected key %s, got %s", string(testData[0].key), string(record.Key))
	}
}

func TestReaderCorruptedKeyData(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")
	fileName := createTestFile(t, testFS, initialData, testData)

	// Read the file and corrupt key bytes of the second record
	f, err := testFS.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("could not open file for corruption: %v", err)
	}
	defer f.Close()

	// Calculate offset to second record's key data
	firstRecordSize := uint32(20) + uint32(len(testData[0].key)) + uint32(len(testData[0].value)) + 4
	secondRecordKeyOffset := int64(len(initialData)) + int64(firstRecordSize) + 20 // skip header

	if _, err := f.Seek(secondRecordKeyOffset, 0); err != nil {
		t.Fatalf("could not seek to key position: %v", err)
	}
	if _, err := f.Write([]byte{0x00}); err != nil {
		t.Fatalf("could not write corrupted key: %v", err)
	}
	f.Close()

	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader: %v", err)
	}
	defer reader.Close()

	// First record should be fine
	record, err := reader.ReadRecordAtStrict(0)
	if err != nil {
		t.Fatalf("error reading first record: %v", err)
	}

	// Second record should fail CRC check due to corrupted key
	offset := record.Size
	if _, err := reader.ReadRecordAtStrict(offset); err != ErrCrcChecksumMismatch {
		t.Errorf("expected ErrCrcChecksumMismatch for corrupted key, got %v", err)
	}
}

func TestReaderMissingData(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")
	fileName := createTestFile(t, testFS, initialData, testData)

	reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
	if err != nil {
		t.Fatalf("error creating reader: %v", err)
	}
	defer reader.Close()

	// Test 1: Truncate in the middle of header
	f, err := testFS.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("could not open file for truncation: %v", err)
	}
	headerTruncatePos := int64(len(initialData)) + 10 // middle of first record header
	f.Truncate(headerTruncatePos)
	f.Close()

	reader, _ = NewReader(testFS, fileName, uint32(len(initialData)))
	defer reader.Close()

	fns := []readerFn{reader.ReadRecordAtStrict, reader.ReadRecordAt, reader.ReadKeyAt, reader.ReadValueAt}
	for _, fn := range fns {
		if _, err := fn(0); err == nil {
			t.Errorf("expected error reading truncated header, got nil")
		}
	}

	// Test 2: Truncate in the middle of key section
	fileName = createTestFile(t, testFS, initialData, testData)
	f, err = testFS.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("could not open file for truncation: %v", err)
	}
	keyTruncatePos := int64(len(initialData)) + 20 + int64(len(testData[0].key))/2 // middle of first record key
	f.Truncate(keyTruncatePos)
	f.Close()

	reader, _ = NewReader(testFS, fileName, uint32(len(initialData)))
	defer reader.Close()

	for _, fn := range fns {
		if _, err := fn(0); err == nil {
			t.Errorf("expected error reading truncated key, got nil")
		}
	}

	// Test 3: Truncate in the middle of value section
	fileName = createTestFile(t, testFS, initialData, testData)
	f, err = testFS.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("could not open file for truncation: %v", err)
	}
	valueTruncatePos := int64(len(initialData)) + 20 + int64(len(testData[0].key)) + int64(len(testData[0].value))/2 // middle of first record value
	f.Truncate(valueTruncatePos)
	f.Close()

	reader, _ = NewReader(testFS, fileName, uint32(len(initialData)))
	defer reader.Close()

	for _, fn := range fns {
		if _, err := fn(0); err == nil {
			t.Errorf("expected error reading truncated value, got nil")
		}
	}
}

func TestReaderMissingDataRandomPositions(t *testing.T) {
	testFS := afero.NewMemMapFs()
	initialData := []byte("somerandominitialdata010101")

	testCases := []struct {
		name           string
		recordIndex    int
		truncateOffset func(initialLen int, recordIndex int, testData []kv) int64
	}{
		{
			name:        "truncate first record header",
			recordIndex: 0,
			truncateOffset: func(initialLen int, recordIndex int, testData []kv) int64 {
				return int64(initialLen) + 7
			},
		},
		{
			name:        "truncate first record key section",
			recordIndex: 0,
			truncateOffset: func(initialLen int, recordIndex int, testData []kv) int64 {
				return int64(initialLen) + 20 + int64(len(testData[0].key))/2
			},
		},
		{
			name:        "truncate first record value section",
			recordIndex: 0,
			truncateOffset: func(initialLen int, recordIndex int, testData []kv) int64 {
				return int64(initialLen) + 20 + int64(len(testData[0].key)) + int64(len(testData[0].value))/2
			},
		},
		{
			name:        "truncate nth record header",
			recordIndex: 3,
			truncateOffset: func(initialLen int, recordIndex int, testData []kv) int64 {
				offset := int64(initialLen)
				for i := 0; i < recordIndex; i++ {
					offset += 20 + int64(len(testData[i].key)) + int64(len(testData[i].value)) + 4
				}
				return offset + 12
			},
		},
		{
			name:        "truncate nth record key section",
			recordIndex: 5,
			truncateOffset: func(initialLen int, recordIndex int, testData []kv) int64 {
				offset := int64(initialLen)
				for i := 0; i < recordIndex; i++ {
					offset += 20 + int64(len(testData[i].key)) + int64(len(testData[i].value)) + 4
				}
				return offset + 20 + int64(len(testData[recordIndex].key))/3
			},
		},
		{
			name:        "truncate nth record value section",
			recordIndex: 7,
			truncateOffset: func(initialLen int, recordIndex int, testData []kv) int64 {
				offset := int64(initialLen)
				for i := 0; i < recordIndex; i++ {
					offset += 20 + int64(len(testData[i].key)) + int64(len(testData[i].value)) + 4
				}
				return offset + 20 + int64(len(testData[recordIndex].key)) + int64(len(testData[recordIndex].value))/2
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fileName := createTestFile(t, testFS, initialData, testData)
			truncatePos := tc.truncateOffset(len(initialData), tc.recordIndex, testData)

			f, err := testFS.OpenFile(fileName, os.O_RDWR, 0666)
			if err != nil {
				t.Fatalf("could not open file for truncation: %v", err)
			}
			f.Truncate(truncatePos)
			f.Close()

			reader, err := NewReader(testFS, fileName, uint32(len(initialData)))
			if err != nil {
				t.Fatalf("error creating reader: %v", err)
			}
			defer reader.Close()

			fns := []struct {
				name string
				fn   readerFn
			}{
				{"ReadRecordAtStrict", reader.ReadRecordAtStrict},
				{"ReadRecordAt", reader.ReadRecordAt},
				{"ReadValueAt", reader.ReadValueAt},
			}

			// Records before truncation should work
			var offset uint32 = 0
			for i := 0; i < tc.recordIndex && i < len(testData); i++ {
				for _, fn := range fns {
					if _, err := fn.fn(offset); err != nil {
						t.Errorf("%s failed for record %d before truncation: %v", fn.name, i, err)
					}
				}
				offset += uint32(20 + len(testData[i].key) + len(testData[i].value) + 4)
			}

			// Record at truncation point should fail for all functions except ReadKeyAt when value is truncated
			for _, fn := range fns {
				_, err := fn.fn(offset)
				if fn.name == "ReadKeyAt" && strings.Contains(tc.name, "value") {
					// ReadKeyAt should succeed even if value is truncated
					continue
				}
				if err == nil {
					t.Errorf("%s expected error for truncated record %d, got nil", fn.name, tc.recordIndex)
				}
			}
		})
	}
}
