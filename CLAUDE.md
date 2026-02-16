# CLAUDE.md

Comprehensive codebase documentation for kvdb - Bitcask-based embedded key-value store with RESP protocol support.

**Module:** `github.com/ananthvk/kvdb` | **Go:** 1.25.5 | **Data Format:** v2.0.0

## Quick Reference

```bash
# Build
go build ./cmd/kvcli       # Interactive REPL
go build ./cmd/kvmake      # Bulk data generator
go build ./cmd/kvjson      # JSON workload generator
go build ./cmd/kvserver    # RESP TCP server (default :6379)

# Test
go test ./...                    # All tests
go test -v ./...                 # Verbose
go test -bench=. ./...           # Benchmarks
go test -run TestMerge ./...     # Specific test

# In-memory mode
kvcli :memory                    # Uses afero.NewMemMapFs()
kvserver -db :memory
```

## Architecture Overview

**Bitcask Model:** Append-only write log + in-memory hash index = O(1) reads/writes

```
┌──────────────────────────────────────────────────────────────┐
│                        DataStore                              │
│  ┌────────────┐  ┌──────────────────────────────────────┐   │
│  │ keydir map │  │            FileManager                │   │
│  │  (O(1))    │  │  ┌─────────┐  ┌──────────────────┐   │   │
│  └────────────┘  │  │ Readers │  │  RotateWriter    │   │   │
│         ↓         │  │  Cache  │  │  (auto-rotate)   │   │   │
│         ↓         │  └─────────┘  └──────────────────┘   │   │
│  Get(key) ────────→│                  ↓                   │   │
│                   │           ┌──────┴──────┐            │   │
│                   │           │             │            │   │
││                ↓ ↓         ↓ ↓         ↓ ↓
│              data/0001.dat  hint/0001.hint  metafile
└─────────────────────────────────────────────────────────────┘
```

## Package Structure

```
github.com/ananthvk/kvdb
├── store.go                    # DataStore API (Get/Put/Delete/Merge/Close)
├── errors.go                   # ErrKeyNotFound, ErrNotExist
├── internal/
│   ├── keydir/                 # In-memory index (map[key]→record)
│   ├── filemanager/            # File rotation, reader pool, merge coordination
│   ├── record/                 # Serialization: Writer/Reader/Scanner + CRC32
│   ├── datafile/               # File header format (19B, version 2.0.0)
│   ├── metafile/               # Metadata: kvdb_store.meta (INI format)
│   ├── hintfile/               # Fast recovery: HintRecord + Writer/Scanner
│   ├── resp/                   # Redis protocol: Value types + ser/deser
│   ├── constants               # MaxKeySize=1KB, MaxValueSize=1MB
│   └── utils                   # File naming: GetDataFileName(id)
└── cmd/
    ├── kvcli                   # REPL (supports :memory)
    ├── kvmake                  # Bulk data generation
    ├── kvjson                  # JSON document workload
    └── kvserver/
        └── internal/           # Server: dispatcher, commands, handler
```

## DataStore API

```go
// Lifecycle
Create(fs afero.Fs, path string) (*DataStore, error)  // New store
Open(fs afero.Fs, path string) (*DataStore, error)    // Existing store
Close() error                                          // Cleanup

// CRUD
Get(key []byte) ([]byte, error)                       // O(1) lookup
Put(key, value []byte) error                          // Append + index update
Delete(key []byte) error                              // Tombstone write
DeleteWithExists(key []byte) (bool, error)            // Delete + exists check

// Utility
ListKeys() []string                                   // All keys
Merge() error                                         // Compact immutable files
Sync() error                                          // Flush buffers
Size() int                                            // Key count
```

## Binary Formats

### Data File (19B header + N records)

```
┌─────────────────────────────────────────────────────────────────┐
│ File Header (19 bytes)                                          │
├─────────────────────────────────────────────────────────────────┤
│ 0-7   │ Magic      │ 0x00 0x6B 0x76 0x64 0x62 0x44 0x41 0x54   │
│ 8-10  │ Version    │ 2.0.0 (major.minor.patch)                  │
│ 11-18 │ Timestamp  │ Unix microseconds (int64 LE)               │
├─────────────────────────────────────────────────────────────────┤
│ Record (variable, repeats)                                      │
├─────────────────────────────────────────────────────────────────┤
│ 0-7   │ Timestamp  │ Unix microseconds (int64 LE)               │
│ 8-11  │ KeySize    │ uint32 LE                                  │
│ 12-15 │ ValueSize  │ uint32 LE                                  │
│ 16    │ Type       │ 0x50=PUT, 0x44=DELETE                     │
│ 17    │ ValueType  │ 0x0 (reserved)                             │
│ 18-19 │ Reserved   │ 0x0000                                    │
│ 20+   │ Key        │ [KeySize] bytes                            │
│ +Key  │ Value      │ [ValueSize] bytes (0 for DELETE)           │
│ -4    │ CRC32      │ IEEE CRC of header+key+value               │
└─────────────────────────────────────────────────────────────────┘
```

**Record Size:** `20 + KeySize + ValueSize + 4` bytes

### Hint File (no header, raw records)

```
┌─────────────────────────────────────────────────────────────────┐
│ HintRecord (24 + KeySize bytes, repeats)                        │
├─────────────────────────────────────────────────────────────────┤
│ 0-7   │ Timestamp  │ Unix microseconds (int64 LE)               │
│ 8-11  │ KeySize    │ uint32 LE                                  │
│ 12-15 │ ValueSize  │ uint32 LE                                  │
│ 16-23 │ ValuePos   │ int64 LE (position in data file)           │
│ 24+   │ Key        │ [KeySize] bytes                            │
└─────────────────────────────────────────────────────────────────┘
```

**Purpose:** Fast keydir rebuild on startup (avoids full datafile scan)

### Meta File (`kvdb_store.meta`, INI format)

```ini
type=kvdb
version=1.0.0
created=2026-02-10 11:32:00.118157 +0000 UTC
max_datafile_size=128000000
```

## File Structure

```
datastore/
├── kvdb_store.meta              # Metadata
├── data/
│   ├── 0000000001.dat           # Zero-padded incrementing IDs
│   ├── 0000000002.dat           # Max ID = active file
│   └── ...
└── hint/
    ├── 0000000001.hint          # One hint per data file
    ├── 0000000002.hint
    └── ...
```

**Merge Temp Files:** `merge-1`, `merge-2`, ... → renamed after completion

## Data Flow Analysis

### Write Path (PUT)

```
DataStore.Put(key, value)
├─→ mu.Lock()                    [Exclusive]
├─→ FileManager.Write(key, value, isTombstone=false)
│   └─→ RotateWriter.Write()
│       ├─→ Check rotation (size > maxDatafileSize)
│       ├─→ record.WriteKeyValue(key, value)
│       │   └─→ Serialize header + key + value + CRC
│       └─→ Append to active file
├─→ keydir.AddKeydirRecord(key, fileId, valueSize, valuePos, ts)
│   └─→ Ignores stale updates (timestamp check)
└─→ mu.Unlock()
```

**Performance:** O(1) append + O(1) hash update

### Read Path (GET)

```
DataStore.Get(key)
├─→ mu.RLock()                   [Shared, allows concurrent readers]
├─→ keydir.GetKeydirRecord(key)  [O(1) hash lookup]
│   └─→ Returns (fileId, valueSize, valuePos, timestamp)
├─→ FileManager.ReadValueAt(fileId, valuePos)
│   ├─→ GetReader(fileId)        [Double-checked locking cache]
│   └─→ record.Reader.ReadValueAt(offset)
│       ├─→ Skip 20B header
│       ├─→ Skip key bytes
│       └─→ Read value bytes + CRC verify
└─→ mu.RUnlock()
```

**Performance:** O(1) index + O(1) pread (direct file access)

### Merge Path (Compaction)

```
DataStore.Merge()
├─→ mergeLock.Lock()             [Single merge at a time]
├─│ Phase 1: Setup
│  └─→ GetImmutableFiles()       [IDs < activeDataFile]
├─│ Phase 2: Scan
│  └─→ For each immutable file:
│      ├─→ record.Scanner.Scan() [4MB shared buffer]
│      ├─→ Check keydir: fileId+pos match?
│      ├─→ Skip if stale/tombstone
│      └─→ Write to merge file + hint file
├─│ Phase 3: Rename
│  ├─→ Sync buffers
│  ├─→ Reserve file IDs (IncrementNextDataFileNumber)
│  └─→ rename merge-N → 000000000N.dat/hint
├─│ Phase 4: Update Index
│  └─→ keydir: Update old fileId→new fileId [Still points to old?]
├─│ Phase 5: Cleanup
│  └─→ Delete old files + CloseAndDeleteReaders()
└─→ mergeLock.Unlock()
```

**Critical:** Active file excluded, concurrent reads allowed (uses mergeLock not mu)

## Concurrency Model

### Lock Hierarchy

```
DataStore.mu (RWMutex)
├─→ RLock: Get, ListKeys, Size, Merge reads
└─→ Lock: Put, Delete, Merge writes

DataStore.mergeLock (Mutex)
└─→ Lock: Merge operation (independent of mu, allows reads during merge)

FileManager.mu (RWMutex)
├─→ Protects: readers map, rotateWriter, file IDs
└─→ Double-checked locking for GetReader cache
```

### Thread-Safety Guarantees

| Operation           | Lock Used     | Concurrent With |
|---------------------|---------------|-----------------|
| Get × Get           | RLock (shared) | ✓               |
| Get × Put           | RLock × Lock  | ✓               |
| Put × Put           | Lock (serial) | ✗               |
| Merge × Get         | mergeLock × RLock | ✓           |
| Merge × Merge       | mergeLock     | ✗               |

### Reader Cache Pattern (Double-Checked Locking)

```go
func (f *FileManager) GetReader(fileId int) (*record.Reader, error) {
    f.mu.RLock()
    reader, exists := f.readers[fileId]
    f.mu.RUnlock()
    if exists { return reader, nil }     // Fast path: lock-free

    f.mu.Lock()
    defer f.mu.Unlock()
    if reader, exists := f.readers[fileId]; exists {
        return reader, nil               // Double-check
    }
    reader := record.NewReader(...)
    f.readers[fileId] = reader
    return reader, nil
}
```

## RESP Protocol Support

### Implemented Commands

| Command | Args     | Response           | Handler          |
|---------|----------|-------------------|------------------|
| PING    | [msg]    | SimpleString PONG | handlePing       |
| ECHO    | msg      | BulkString msg    | handleEcho       |
| GET     | key      | BulkString/Null   | handleGet        |
| SET     | key val  | SimpleString OK   | handleSet        |
| KEYS    | pattern  | Array of keys     | handleKeys (*)   |
| DEL     | key...   | Integer count     | handleDel        |

**Limitation:** KEYS ignores pattern (always returns all keys sorted)

### Server Usage

```bash
kvserver -db /path/to/db -host 0.0.0.0 -port 6379
```

**Background Tasks:**
- Sync: Every 30s
- Merge: Every 2min

**TODO:** No cancellation for goroutines on Close

## CLI Tools

### kvcli (Interactive REPL)

```
Usage: kvcli <path> | :memory

Commands:
  key=value          SET operation
  key                GET operation
  \keys              List all keys
  \scan              List all key=value pairs
  \delete <key>      Delete key
  \size              Show key count
  \sync              Flush to disk
  \merge             Trigger merge (async)
  \seed              Insert test data
  exit               Quit
```

### kvmake (Bulk Data)

```bash
kvmake -n 10000 ./mydb
```

### kvjson (JSON Workload)

```bash
kvjson -n 10000 -size 1024 -db ./mydb
```

**Model:** UserProfile with ID, Username, Email, Age, Tags, Metadata, Payload

## Constants & Limits

| Constant            | Value              | Location              |
|---------------------|--------------------|-----------------------|
| MaxKeySize          | 1000 bytes (1 KB)  | internal/constants    |
| MaxValueSize        | 1,000,000 bytes (1 MB) | internal/constants    |
| MaxBulkStringSize   | 1,048,576 bytes (1 MiB) | internal/resp        |
| defaultMaxDatafileSize | 128,000,000 bytes (128 MB) | store.go        |
| readerBufferSize    | 4,194,304 bytes (4 MB) | record/scanner.go   |
| writerBufferSize    | 4,194,304 bytes (4 MB) | record/writer.go    |

## Error Types

```go
// Root package
var (
    ErrKeyNotFound = errors.New("key not found")
    ErrNotExist    = errors.New("datastore does not exist")
)

// Record package
var (
    ErrCrcChecksumMismatch = errors.New("crc checksum does not match")
    ErrKeyTooLarge         = errors.New("key too large")
    ErrValueTooLarge       = errors.New("value too large")
)

// RESP package
var (
    ErrProtocolError    = errors.New("protocol error")
    ErrTooLarge         = errors.New("bulk string length too large")
    ErrUnknownValueType = errors.New("unknown value type")
)

// Datafile package
var (
    ErrNotDataFile                  = errors.New("not a kvdb data file")
    ErrDataFileVersionNotCompatible = errors.New("datafile not supported")
)
```

## Critical Implementation Details

### Keydir Timestamp-based Stale Detection
- `AddKeydirRecord()` ignores updates with older timestamps
- Prevents race conditions during merge and concurrent updates

### Shared Buffer Optimization (Scanner)
- Key/Value slices backed by shared 4MB buffer
- **Must copy data** if needed after next Scan()
- Reduces allocations during merge

### Active File Identification
- On startup: Find max numerical ID in `data/`
- All other files are immutable
- New file created on each restart (TODO: inefficient)

### Hint File Strategy
- Written during merge (one per output file)
- Read during startup if exists (fast path)
- Falls back to datafile scan if missing/error
- **TODO:** No header for integrity checks

### ValuePos Semantics
- `KeydirRecord.ValuePos`: Offset to **RECORD start** (not value start)
- Must subtract `datafile.FileHeaderSize` in Get
- Offsets start from first record (after 19B header)

### Tombstone Handling
- `RecordTypeDelete = 0x44`: Key present, ValueSize=0, no value bytes
- Removed from keydir immediately on Delete()
- Skipped during merge compaction

## Testing Patterns

### Test Types

| Type        | Pattern                          | Examples                     |
|-------------|----------------------------------|------------------------------|
| Unit        | afero.NewMemMapFs()              | record, resp, filemanager    |
| Integration | afero.NewOsFs() + temp dirs      | store merge, concurrent ops  |
| Benchmark   | b.N iterations                   | store_benchmark_test.go      |

### Key Test Files

- `store_test.go` (435 lines): CRUD, merge, persistence
- `concurrent_operations_and_merge_test.go` (583 lines): Real filesystem, multiple rotations, concurrent merges
- `record/*_test.go` (726 lines): CRC, serialization, scanner, reader
- `resp/*_test.go` (1493 lines): Protocol compliance, error handling

## Known Issues / TODOs

1. **File rotation on restart** - Always creates new file (inefficient)
2. **Hint file corruption** - No integrity checks (TODO in code)
3. **Background goroutine leaks** - No cancellation in kvserver
4. **KEYS pattern** - Ignores pattern parameter (only supports `*`)
5. **Merge error handling** - Stops on first file error (no skip-and-continue)
6. **Multi-process support** - Single-process only
7. **TTL** - No record expiration
8. **Crash recovery** - No recovery from incomplete writes

## Design Strengths

1. **Clean separation** - Package boundaries (keydir, record, filemanager, hintfile)
2. **Interface-based** - afero.Fs enables memory filesystem testing
3. **Concurrent-safe** - RWMutex allows multiple readers
4. **Crash-resistant** - Append-only + CRC32 checksums
5. **Fast recovery** - Hint files avoid full datafile scan
6. **Efficient reads** - O(1) index + pread (no read-ahead)
7. **Space reclamation** - Merge removes stale data

## Design Trade-offs

1. **Memory usage** - Entire keyset in memory (not for huge datasets)
2. **Startup time** - Must rebuild index (mitigated by hint files)
3. **Write amplification** - Old values remain until merge
4. **Merge cost** - CPU/I/O intensive during compaction
5. **File permutations** - New file on restart (wastes file slots)

## Keydir Index Structure

```go
type KeydirRecord struct {
    FileId    int       // Which data file
    ValueSize uint32    // Value length (bytes)
    ValuePos  int64     // Offset from first record (after header)
    Timestamp time.Time // For stale detection
}

keydir: map[string]KeydirRecord
```

**Note:** `ValuePos` is offset from record start, not file start. Subtract `FileHeaderSize (19B)` for file offset.

## RESP Value Types

```go
type ValueType int
const (
    ValueTypeNull         // "$-1\r\n"
    ValueTypeSimpleString // "+OK\r\n"
    ValueTypeSimpleError  // "-ERR msg\r\n"
    ValueTypeInteger      // ":123\r\n"
    ValueTypeBulkString   // "$6\r\nfoobar\r\n"
    ValueTypeArray        // "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
)
```

**Max BulkString:** 1 MiB (enforced in deserializer)
