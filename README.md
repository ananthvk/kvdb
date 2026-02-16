# kvdb

A simple, fast persistent key-value store based on [Bitcask](https://riak.com/assets/bitcask-intro.pdf), and a TCP server compatible with the Redis protocol (RESP)

## How to run

### Install

```
$ go install github.com/ananthvk/kvdb/cmd/kvserver@latest
```

Then use it as,

```
$ kvserver -db mydb
```

Or, clone this repository, and then run the following commands

### Standalone CLI

```
$ go run ./cmd/kvcli <path to database directory>
```

### To run the redis compatible server

```
$ go run ./cmd/kvserver -db <path to db directory> -host 0.0.0.0 -port 6379
```

For example,
```
$ go run ./cmd/kvserver -db mydb
```

Access the server through `redis-cli`

Supported commands: `GET`, `SET`, `ECHO`, `PING`, `KEYS *`, `DEL`

### To create dummy data,

```
$ go run ./cmd/kvjson -n 100000 -size 1024 -db testdb
```

or 

```
$ go run ./cmd/kvmake -n 5000_000 5mdb
```

## Features

- Merge and compaction to remove stale keys, and merge datafiles
- Hint files to improve startup time
- Redis (RESP) compatible TCP server
- Supports multiple readers, and a single writer (same process)
- Log rotation determined by log file size


## File format specification for datafile

All integer values are stored in Little Endian format

| Name          | Offset | Size (bytes) | Type                                      | Comments                                                                                                                                                            |
| ------------- | ------ | ------------ | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Magic number  | 0      | 8            | `0x00 0x6b 0x76 0x64 0x62 0x44 0x41 0x54` | Identifies the file(`0x0` followed by `kvdb`, `DAT` represents that it's a data file)                                                                               |
| Version major | 8      | 1            | uint8_t                                   | Major version of the file format                                                                                                                                    |
| Version minor | 9      | 1            | uint8_t                                   | Minor version of the file format                                                                                                                                    |
| Version patch | 10     | 1            | uint8_t                                   | Patch version of the file format                                                                                                                                    |
| Timestamp     | 11     | 8            | int64_t                                  | Timestamp of file creation                                                                                                                                          |

The file header is `19 bytes` in size

### Log Format

Each log entry in the data file contains a log header (`20 bytes`) followed by variable length key and value, followed by the CRC of the header + key + value

Log record:

| Name        | Offset | Size (bytes)  | Type     | Comments                                                           |
| ----------- | ------ | ------------- | -------- | ------------------------------------------------------------------ |
| Timestamp   | 0      | 8             | int64_t | Timestamp of log entry (for debug / informational)                 |
| Key size    | 8      | 4             | uint32_t | Size of the key (Note: this restricts max key size to around 4Gib) |
| Value size  | 12     | 4             | uint32_t | Size of the value (Same restriction as above)                      |
| Record type | 16     | 1             | uint8_t  | Type of record                                                     |
| Value type  | 17     | 1             | uint8_t  | Type of value (future use)                                         |
| Reserved    | 18     | 2             | uint16_t | Reserved for future use                                            |
| Key         | 20     | Variable size | byte seq | Key                                                                |
| Value<br>   | -      | Variable size | byte seq | Value                                                              |
| CRC         | -      | 4             | uint32_t | CRC covers record header + key + value       |

Note: Timestamps are unix timestamps, in microsecond format

Record type
```
0x50 ('P') - PUT record
0x44 ('D') - DELETE (Tombstone), Value Size is set to 0, and no value bytes are present
```

## Directory structure

```
testdb/
	kvdb_store.meta
	data/
		0000000001.dat
		0000000002.dat
		...
```

The file with the largest numerical value is considered the active file when opening the datastore.

The file name is zero padded to length of 10 characters


A file `kvdb_store.meta` will indicate that the directory is a valid store, it also holds configuration of the datastore

It follows a simple INI like key=value structure

Example structure
```
name = testdb
version = 1.0.0
created = 2026-02-10 11:32:00.118157 +0000 UTC
max_datafile_size=5000000
```

## Key & Value size limits

Keys have a maximum size of `1000 bytes (1 KB)`

And values have a maximum size of `1000000 bytes  (1 MB)`

Default value of max data file size is `12800000 bytes (128MB)` but it's configurable through `kvdb_store.meta` file

## TODO

- [ ] Handling of corrupted records & hint file
- [ ] Crash recovery
- [ ] Add support for multiple processes
- [ ] Implement TTL
- [ ] Graceful shutdown 