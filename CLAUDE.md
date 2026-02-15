# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

kvdb is a fast, embedded key-value store based on the Bitcask storage model (similar to Riak's Bitcask). It uses an append-only log structure with an in-memory index for O(1) lookups.

**Module path:** `github.com/ananthvk/kvdb`

## Build and Test Commands

```bash
# Build the CLI tools
go build ./cmd/kvcli      # Interactive CLI for store operations
go build ./cmd/kvmake     # Bulk data generation utility

# Run tests
go test ./...              # Run all tests
go test -v ./...           # Run with verbose output
go test -bench=. ./...     # Run benchmarks

# Run tests for specific package
go test ./internal/keydir
go test -run TestMerge     # Run specific test
```

## Architecture

The Bitcask architecture separates write operations (append-only) from read operations (in-memory index):

### Core Components

- **DataStore** (`store.go`): Main entry point providing Get/Put/Delete/ListKeys/Merge operations. Thread-safe with RWMutex for concurrent access.
- **keydir** (`internal/keydir/`): In-memory hash map storing key → (fileId, position, size, timestamp). Provides O(1) key lookups.
- **filemanager** (`internal/filemanager/`): Manages data file rotation, concurrent reads, and write operations. Handles merging/compaction.
- **record** (`internal/record/`): Record serialization/deserialization with CRC32 checksums for data integrity.
- **datafile** (`internal/datafile/`): Data file format handling (19-byte header, 20-byte record headers).
- **metafile** (`internal/metafile/`): Store metadata in INI-like format (`kvdb_store.meta`).

### Data Flow

**Write Path (Put/Delete):**
1. Serialize record with header (timestamp, key size, value size, type) + CRC
2. Append to current active data file
3. Update in-memory keydir
4. Rotate file if size limit reached

**Read Path (Get):**
1. Look up key in keydir (in-memory, O(1))
2. If found, read value directly from file at stored position
3. Verify CRC before returning

**Merge/Compaction:**
1. Scan all immutable data files
2. Keep only active records (checking keydir for staleness)
3. Write to new temporary files
4. Swap old files with new compacted files
5. Update keydir with new file IDs

### File Structure

```
datastore/
├── kvdb_store.meta          # Metadata (INI format: name, version, size limits)
└── data/
    ├── 0000000001.dat       # Data files (zero-padded, incrementing IDs)
    ├── 0000000002.dat
    └── ...
```

**Active file:** File with largest numerical ID when store is opened.

### Binary Format

All integers are Little Endian. Each data file:
- **Header** (19 bytes): Magic number (`kvdbDAT`) + version (3 bytes) + timestamp (8 bytes)
- **Records**: Header (20 bytes) + key + value + CRC (4 bytes)
  - Record header: timestamp (8) + key size (4) + value size (4) + type (1) + value type (1) + reserved (2)
  - Types: `0x50` (PUT), `0x44` (DELETE/tombstone)

### Key Conventions

- **Filesystem abstraction:** Uses `afero.Fs` interface for testability. Pass `afero.NewOsFs()` for real filesystem.
- **Concurrency:**
  - `mu` (RWMutex): Protects keydir and read operations
  - `mergeLock` (Mutex): Ensures only one merge at a time
- **Error handling:** Check for `ErrKeyNotFound` and `ErrNotExist` from `errors.go`
- **Timestamps:** Unix microseconds, used for merge conflict resolution


## Implementation Notes

- Merges operate on immutable files only; active file is excluded
- During merge, keydir is checked to verify records are still active (fileId + position match)
- File rotation happens synchronously when size limit is reached
- The CLI (`kvcli`) provides convenient commands: `\keys`, `\scan`, `\merge`, `\delete <key>`, `\seed`
