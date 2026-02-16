package resp

import (
	"bufio"
	"bytes"
	"math"
	"testing"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		wantErr bool
		errType error
	}{
		// Simple String tests
		{
			name: "simple string - valid",
			value: Value{
				Type:   ValueTypeSimpleString,
				Buffer: []byte("OK"),
			},
			wantErr: false,
		},
		{
			name: "simple string - empty",
			value: Value{
				Type:   ValueTypeSimpleString,
				Buffer: []byte(""),
			},
			wantErr: false,
		},
		{
			name: "simple string - with \\r",
			value: Value{
				Type:   ValueTypeSimpleString,
				Buffer: []byte("test\rvalue"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		{
			name: "simple string - with \\n",
			value: Value{
				Type:   ValueTypeSimpleString,
				Buffer: []byte("test\nvalue"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		{
			name: "simple string - with \\r\\n",
			value: Value{
				Type:   ValueTypeSimpleString,
				Buffer: []byte("test\r\nvalue"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		// Simple Error tests
		{
			name: "simple error - valid",
			value: Value{
				Type:              ValueTypeSimpleError,
				SimpleErrorPrefix: []byte("ERR"),
				Buffer:            []byte("something went wrong"),
			},
			wantErr: false,
		},
		{
			name: "simple error - empty prefix and content",
			value: Value{
				Type:              ValueTypeSimpleError,
				SimpleErrorPrefix: []byte(""),
				Buffer:            []byte(""),
			},
			wantErr: false,
		},
		{
			name: "simple error - prefix with \\r",
			value: Value{
				Type:              ValueTypeSimpleError,
				SimpleErrorPrefix: []byte("ERR\r"),
				Buffer:            []byte("error"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		{
			name: "simple error - prefix with \\n",
			value: Value{
				Type:              ValueTypeSimpleError,
				SimpleErrorPrefix: []byte("ERR\n"),
				Buffer:            []byte("error"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		{
			name: "simple error - content with \\r",
			value: Value{
				Type:              ValueTypeSimpleError,
				SimpleErrorPrefix: []byte("ERR"),
				Buffer:            []byte("error\rmessage"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		{
			name: "simple error - content with \\n",
			value: Value{
				Type:              ValueTypeSimpleError,
				SimpleErrorPrefix: []byte("ERR"),
				Buffer:            []byte("error\nmessage"),
			},
			wantErr: true,
			errType: ErrInvalidValue,
		},
		// Integer tests
		{
			name: "integer - zero",
			value: Value{
				Type:    ValueTypeInteger,
				Integer: 0,
			},
			wantErr: false,
		},
		{
			name: "integer - negative one",
			value: Value{
				Type:    ValueTypeInteger,
				Integer: -1,
			},
			wantErr: false,
		},
		{
			name: "integer - positive one",
			value: Value{
				Type:    ValueTypeInteger,
				Integer: 1,
			},
			wantErr: false,
		},
		{
			name: "integer - max int64",
			value: Value{
				Type:    ValueTypeInteger,
				Integer: math.MaxInt64,
			},
			wantErr: false,
		},
		{
			name: "integer - min int64",
			value: Value{
				Type:    ValueTypeInteger,
				Integer: math.MinInt64,
			},
			wantErr: false,
		},
		// Bulk String tests
		{
			name: "bulk string - valid",
			value: Value{
				Type:   ValueTypeBulkString,
				Buffer: []byte("hello world"),
			},
			wantErr: false,
		},
		{
			name: "bulk string - empty",
			value: Value{
				Type:   ValueTypeBulkString,
				Buffer: []byte(""),
			},
			wantErr: false,
		},
		{
			name: "bulk string - with \\r",
			value: Value{
				Type:   ValueTypeBulkString,
				Buffer: []byte("test\rvalue"),
			},
			wantErr: false,
		},
		{
			name: "bulk string - with \\n",
			value: Value{
				Type:   ValueTypeBulkString,
				Buffer: []byte("test\nvalue"),
			},
			wantErr: false,
		},
		{
			name: "bulk string - with \\r\\n",
			value: Value{
				Type:   ValueTypeBulkString,
				Buffer: []byte("test\r\nvalue"),
			},
			wantErr: false,
		},
		// Null tests
		{
			name: "null",
			value: Value{
				Type: ValueTypeNull,
			},
			wantErr: false,
		},
		// Array tests
		{
			name: "array - empty",
			value: Value{
				Type:  ValueTypeArray,
				Array: []Value{},
			},
			wantErr: false,
		},
		{
			name: "array - simple strings",
			value: Value{
				Type: ValueTypeArray,
				Array: []Value{
					{Type: ValueTypeSimpleString, Buffer: []byte("hello")},
					{Type: ValueTypeSimpleString, Buffer: []byte("world")},
				},
			},
			wantErr: false,
		},
		{
			name: "array - mixed types",
			value: Value{
				Type: ValueTypeArray,
				Array: []Value{
					{Type: ValueTypeSimpleString, Buffer: []byte("OK")},
					{Type: ValueTypeInteger, Integer: 42},
					{Type: ValueTypeBulkString, Buffer: []byte("test\r\ndata")},
					{Type: ValueTypeNull},
				},
			},
			wantErr: false,
		},
		{
			name: "array - nested arrays",
			value: Value{
				Type: ValueTypeArray,
				Array: []Value{
					{Type: ValueTypeSimpleString, Buffer: []byte("outer")},
					{
						Type: ValueTypeArray,
						Array: []Value{
							{Type: ValueTypeSimpleString, Buffer: []byte("inner1")},
							{Type: ValueTypeSimpleString, Buffer: []byte("inner2")},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "array - with nulls",
			value: Value{
				Type: ValueTypeArray,
				Array: []Value{
					{Type: ValueTypeNull},
					{Type: ValueTypeSimpleString, Buffer: []byte("test")},
					{Type: ValueTypeNull},
				},
			},
			wantErr: false,
		},
		// Invalid type test
		{
			name: "invalid type",
			value: Value{
				Type: ValueType(99),
			},
			wantErr: true,
			errType: ErrInvalidType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := bufio.NewWriter(&buf)

			err := Serialize(tt.value, w)
			if (err != nil) != tt.wantErr {
				t.Errorf("Serialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				if tt.errType != nil && err != tt.errType {
					t.Errorf("Serialize() error = %v, want %v", err, tt.errType)
				}
				return
			}

			// Flush the writer
			if err := w.Flush(); err != nil {
				t.Fatalf("Failed to flush writer: %v", err)
			}

			// Deserialize and verify
			r := bufio.NewReader(&buf)
			deserialized, err := Deserialize(r)
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}

			// Compare values
			if !compareValues(tt.value, deserialized) {
				t.Errorf("Deserialized value does not match original.\nOriginal: %+v\nDeserialized: %+v", tt.value, deserialized)
			}
		})
	}
}

func compareValues(v1, v2 Value) bool {
	if v1.Type != v2.Type {
		return false
	}

	switch v1.Type {
	case ValueTypeNull:
		return true
	case ValueTypeSimpleString, ValueTypeBulkString:
		return bytes.Equal(v1.Buffer, v2.Buffer)
	case ValueTypeSimpleError:
		return bytes.Equal(v1.SimpleErrorPrefix, v2.SimpleErrorPrefix) && bytes.Equal(v1.Buffer, v2.Buffer)
	case ValueTypeInteger:
		return v1.Integer == v2.Integer
	case ValueTypeArray:
		if len(v1.Array) != len(v2.Array) {
			return false
		}
		for i := range v1.Array {
			if !compareValues(v1.Array[i], v2.Array[i]) {
				return false
			}
		}
		return true
	}
	return false
}
