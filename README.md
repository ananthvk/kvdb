# kvdb

A fast key-value store based on [Bitcask](https://riak.com/assets/bitcask-intro.pdf)


## File format specification

All integer values are stored in Little Endian format

| Name          | Offset | Size (bytes) | Type                                      | Comments                                                                                                                                                            |
| ------------- | ------ | ------------ | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Magic number  | 0      | 8            | `0x00 0x6b 0x76 0x64 0x62 0x44 0x41 0x54` | Identifies the file(`0x0` followed by `kvdb`, `DAT` represents that it's a data file)                                                                               |
| Version major | 8      | 1            | uint8_t                                   | Major version of the file format                                                                                                                                    |
| Version minor | 9      | 1            | uint8_t                                   | Minor version of the file format                                                                                                                                    |
| Version patch | 10     | 1            | uint8_t                                   | Patch version of the file format                                                                                                                                    |
| Timestamp     | 11     | 8            | int64_t                                  | Timestamp of file creation                                                                                                                                          |
| Offset        | 19     | 4            | uint32_t                                  | Offset to the first log record (this keeps the header flexible, and extra fields can be added after the header, by default, offset is set to just after the header) |
| Reserved      | 23     | 1            | uint8_t                                   | Reserved                                                                                                                                                            |

The file header is `24 bytes` in size

### Log Format

Each log entry in the data file contains a log header (`24 bytes`) followed by variable length key and value

Log record:

| Name        | Offset | Size (bytes)  | Type     | Comments                                                           |
| ----------- | ------ | ------------- | -------- | ------------------------------------------------------------------ |
| CRC         | 0      | 4             | uint32_t | CRC covers record header (excluding CRC field) + key + value       |
| Timestamp   | 4      | 8             | int64_t | Timestamp of log entry (for debug / informational)                 |
| Key size    | 12     | 4             | uint32_t | Size of the key (Note: this restricts max key size to around 4Gib) |
| Value size  | 16     | 4             | uint32_t | Size of the value (Same restriction as above)                      |
| Record type | 20     | 1             | uint8_t  | Type of record                                                     |
| Value type  | 21     | 1             | uint8_t  | Type of value (future use)                                         |
| Reserved    | 22     | 2             | uint16_t | Reserved for future use                                            |
| Key         | 24     | Variable size | byte seq | Key                                                                |
| Value<br>   | -      | Variable size | byte seq | Value                                                              |

Note: Timestamps are unix timestamps, in microsecond format

Record type
```
0x50 ('P') - PUT record
0x44 ('D') - DELETE (Tombstone), Value Size is set to 0, and no value bytes are present
```