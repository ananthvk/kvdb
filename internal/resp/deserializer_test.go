package resp

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestDeserializeSimpleString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr error
	}{
		{
			name:    "empty string",
			input:   "\r\n",
			want:    "",
			wantErr: nil,
		},
		{
			name:    "single char string",
			input:   "a\r\n",
			want:    "a",
			wantErr: nil,
		},
		{
			name:    "newline before carriage return",
			input:   "hello\nworld\r\n",
			want:    "",
			wantErr: ErrProtocolError,
		},
		{
			name:    "string with carriage return but no newline",
			input:   "hello\r",
			want:    "",
			wantErr: io.EOF,
		},
		{
			name:    "string with carriage return but other character after",
			input:   "hello\ra",
			want:    "",
			wantErr: ErrProtocolError,
		},
		{
			name:    "normal test case",
			input:   "hello world\r\n",
			want:    "hello world",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := DeserializeSimpleString(r)

			if err != tt.wantErr {
				t.Errorf("DeserializeSimpleString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil && string(got.Buffer) != tt.want {
				t.Errorf("DeserializeSimpleString() = %v, want %v", string(got.Buffer), tt.want)

				if got.Type != ValueTypeSimpleString {
					t.Errorf("DeserializeSimpleString() Type = %v, want %v", got.Type, ValueTypeSimpleString)
				}
			}
		})
	}
}

func TestDeserializeError(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantPrefix  string
		wantMessage string
		wantErr     error
	}{
		{
			name:        "single character",
			input:       "E\r\n",
			wantPrefix:  "E",
			wantMessage: "E",
			wantErr:     nil,
		},
		{
			name:        "single word",
			input:       "ERR\r\n",
			wantPrefix:  "ERR",
			wantMessage: "ERR",
			wantErr:     nil,
		},
		{
			name:        "two words separated by space",
			input:       "ERR message\r\n",
			wantPrefix:  "ERR",
			wantMessage: "message",
			wantErr:     nil,
		},
		{
			name:        "multiple words separated by space",
			input:       "ERR this is an error message\r\n",
			wantPrefix:  "ERR",
			wantMessage: "this is an error message",
			wantErr:     nil,
		},
		{
			name:        "first char is space",
			input:       " ERR message\r\n",
			wantPrefix:  "",
			wantMessage: "ERR message",
			wantErr:     nil,
		},
		{
			name:        "multiple spaces",
			input:       "ERR  multiple  spaces\r\n",
			wantPrefix:  "ERR",
			wantMessage: " multiple  spaces",
			wantErr:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			gotError, err := DeserializeError(r)
			gotPrefix := gotError.SimpleErrorPrefix
			gotMessage := gotError.Buffer

			if err != tt.wantErr {
				t.Errorf("DeserializeError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if string(gotPrefix) != tt.wantPrefix {
					t.Errorf("DeserializeError() prefix = %v, want %v", string(gotPrefix), tt.wantPrefix)
				}
				if string(gotMessage) != tt.wantMessage {
					t.Errorf("DeserializeError() message = %v, want %v", string(gotMessage), tt.wantMessage)
				}
				if gotError.Type != ValueTypeSimpleError {
					t.Errorf("DeserializeError() Type = %v, want %v", gotError.Type, ValueTypeSimpleError)
				}
			}
		})
	}
}
func TestDeserializeBulkString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantNil bool
		wantErr error
	}{
		{
			name:    "empty string",
			input:   "0\r\n\r\n",
			want:    "",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "null string",
			input:   "-1\r\n",
			want:    "",
			wantNil: true,
			wantErr: nil,
		},
		{
			name:    "single character string",
			input:   "1\r\na\r\n",
			want:    "a",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "two character string",
			input:   "2\r\nab\r\n",
			want:    "ab",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "larger string",
			input:   "11\r\nhello world\r\n",
			want:    "hello world",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "string containing binary data",
			input:   "5\r\n\x00\x01\x02\x03\x04\r\n",
			want:    "\x00\x01\x02\x03\x04",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "string containing emojis",
			input:   "8\r\n😀🎉\r\n",
			want:    "😀🎉",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "string with newline in content",
			input:   "11\r\nhello\nworld\r\n",
			want:    "hello\nworld",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "string with carriage return in content",
			input:   "11\r\nhello\rworld\r\n",
			want:    "hello\rworld",
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "string with CRLF in content",
			input:   "12\r\nhello\r\nworld\r\n",
			want:    "hello\r\nworld",
			wantNil: false,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := DeserializeBulkString(r)

			if err != tt.wantErr {
				t.Errorf("DeserializeBulkString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if tt.wantNil {
					if got.Buffer != nil {
						t.Errorf("DeserializeBulkString() = %v, want nil", got)
					}
				} else {
					if string(got.Buffer) != tt.want {
						t.Errorf("DeserializeBulkString() = %v, want %v", string(got.Buffer), tt.want)
					}
					if got.Type != ValueTypeBulkString {
						t.Errorf("DeserializeBulkString() Type = %v, want %v", got.Type, ValueTypeBulkString)
					}
				}
			}
		})
	}
}

func TestDeserializeInteger(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr error
	}{
		{
			name:    "zero",
			input:   "0\r\n",
			want:    0,
			wantErr: nil,
		},
		{
			name:    "positive one",
			input:   "1\r\n",
			want:    1,
			wantErr: nil,
		},
		{
			name:    "negative one",
			input:   "-1\r\n",
			want:    -1,
			wantErr: nil,
		},
		{
			name:    "negative huge number",
			input:   "-9223372036854775807\r\n",
			want:    -9223372036854775807,
			wantErr: nil,
		},
		{
			name:    "positive huge number",
			input:   "9223372036854775807\r\n",
			want:    9223372036854775807,
			wantErr: nil,
		},
		{
			name:    "number with plus sign",
			input:   "+42\r\n",
			want:    42,
			wantErr: nil,
		},
		{
			name:    "number with minus sign",
			input:   "-42\r\n",
			want:    -42,
			wantErr: nil,
		},
		{
			name:    "invalid number with letters",
			input:   "12a34\r\n",
			want:    0,
			wantErr: ErrProtocolError,
		},
		{
			name:    "invalid number with symbols",
			input:   "12@34\r\n",
			want:    0,
			wantErr: ErrProtocolError,
		},
		{
			name:    "invalid number with special char",
			input:   "12#34\r\n",
			want:    0,
			wantErr: ErrProtocolError,
		},
		{
			name:    "number with newline in between",
			input:   "12\n34\r\n",
			want:    0,
			wantErr: ErrProtocolError,
		},
		{
			name:    "missing newline after carriage return",
			input:   "42\r",
			want:    0,
			wantErr: io.EOF,
		},
		{
			name:    "wrong character after carriage return",
			input:   "42\ra",
			want:    0,
			wantErr: ErrProtocolError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := DeserializeInteger(r)

			if err != tt.wantErr {
				t.Errorf("DeserializeInteger() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if got.Integer != tt.want {
					t.Errorf("DeserializeInteger() = %v, want %v", got.Integer, tt.want)
				}
				if got.Type != ValueTypeInteger {
					t.Errorf("DeserializeInteger() Type = %v, want %v", got.Type, ValueTypeInteger)
				}
			}
		})
	}
}

func TestDeserializeArray(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantNil  bool
		validate func(*testing.T, Value)
		wantErr  error
	}{
		{
			name:    "empty array",
			input:   "0\r\n",
			wantLen: 0,
			wantNil: false,
			wantErr: nil,
		},
		{
			name:    "null array",
			input:   "-1\r\n",
			wantLen: 0,
			wantNil: true,
			wantErr: nil,
		},
		{
			name:    "invalid negative length",
			input:   "-5\r\n",
			wantLen: 0,
			wantErr: ErrProtocolError,
		},
		{
			name:    "array with single simple string",
			input:   "1\r\n+hello\r\n",
			wantLen: 1,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if v.Array[0].Type != ValueTypeSimpleString {
					t.Errorf("expected simple string type")
				}
				if string(v.Array[0].Buffer) != "hello" {
					t.Errorf("got %s, want hello", string(v.Array[0].Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:    "array with single integer",
			input:   "1\r\n:42\r\n",
			wantLen: 1,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if v.Array[0].Type != ValueTypeInteger {
					t.Errorf("expected integer type")
				}
				if v.Array[0].Integer != 42 {
					t.Errorf("got %d, want 42", v.Array[0].Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:    "array with single bulk string",
			input:   "1\r\n$5\r\nhello\r\n",
			wantLen: 1,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if v.Array[0].Type != ValueTypeBulkString {
					t.Errorf("expected bulk string type")
				}
				if string(v.Array[0].Buffer) != "hello" {
					t.Errorf("got %s, want hello", string(v.Array[0].Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:    "array with two elements",
			input:   "2\r\n+hello\r\n:123\r\n",
			wantLen: 2,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if string(v.Array[0].Buffer) != "hello" {
					t.Errorf("got %s, want hello", string(v.Array[0].Buffer))
				}
				if v.Array[1].Integer != 123 {
					t.Errorf("got %d, want 123", v.Array[1].Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:    "array with five mixed elements",
			input:   "5\r\n+hello\r\n:42\r\n$5\r\nworld\r\n-ERR error\r\n:99\r\n",
			wantLen: 5,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if string(v.Array[0].Buffer) != "hello" {
					t.Errorf("element 0: got %s, want hello", string(v.Array[0].Buffer))
				}
				if v.Array[1].Integer != 42 {
					t.Errorf("element 1: got %d, want 42", v.Array[1].Integer)
				}
				if string(v.Array[2].Buffer) != "world" {
					t.Errorf("element 2: got %s, want world", string(v.Array[2].Buffer))
				}
				if v.Array[3].Type != ValueTypeSimpleError {
					t.Errorf("element 3: expected error type")
				}
				if v.Array[4].Integer != 99 {
					t.Errorf("element 4: got %d, want 99", v.Array[4].Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:    "array within an array",
			input:   "2\r\n+hello\r\n*2\r\n:1\r\n:2\r\n",
			wantLen: 2,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if string(v.Array[0].Buffer) != "hello" {
					t.Errorf("element 0: got %s, want hello", string(v.Array[0].Buffer))
				}
				if v.Array[1].Type != ValueTypeArray {
					t.Errorf("element 1: expected array type")
				}
				if len(v.Array[1].Array) != 2 {
					t.Errorf("nested array length: got %d, want 2", len(v.Array[1].Array))
				}
				if v.Array[1].Array[0].Integer != 1 {
					t.Errorf("nested element 0: got %d, want 1", v.Array[1].Array[0].Integer)
				}
				if v.Array[1].Array[1].Integer != 2 {
					t.Errorf("nested element 1: got %d, want 2", v.Array[1].Array[1].Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:    "multiple arrays within an array",
			input:   "3\r\n*2\r\n:1\r\n:2\r\n*1\r\n+hello\r\n*0\r\n",
			wantLen: 3,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				if len(v.Array[0].Array) != 2 {
					t.Errorf("array 0 length: got %d, want 2", len(v.Array[0].Array))
				}
				if len(v.Array[1].Array) != 1 {
					t.Errorf("array 1 length: got %d, want 1", len(v.Array[1].Array))
				}
				if len(v.Array[2].Array) != 0 {
					t.Errorf("array 2 length: got %d, want 0", len(v.Array[2].Array))
				}
			},
			wantErr: nil,
		},
		{
			name:    "array of five bulk strings",
			input:   "5\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$4\r\nfour\r\n$4\r\nfive\r\n",
			wantLen: 5,
			wantNil: false,
			validate: func(t *testing.T, v Value) {
				expected := []string{"one", "two", "three", "four", "five"}
				for i, exp := range expected {
					if v.Array[i].Type != ValueTypeBulkString {
						t.Errorf("element %d: expected bulk string type", i)
					}
					if string(v.Array[i].Buffer) != exp {
						t.Errorf("element %d: got %s, want %s", i, string(v.Array[i].Buffer), exp)
					}
				}
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := DeserializeArray(r)

			if err != tt.wantErr {
				t.Errorf("DeserializeArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if tt.wantNil {
					if got.Array != nil {
						t.Errorf("DeserializeArray() = %v, want nil", got)
					}
				} else {
					if len(got.Array) != tt.wantLen {
						t.Errorf("DeserializeArray() length = %d, want %d", len(got.Array), tt.wantLen)
					}
					if got.Type != ValueTypeArray {
						t.Errorf("DeserializeArray() Type = %v, want %v", got.Type, ValueTypeArray)
					}
					if tt.validate != nil {
						tt.validate(t, got)
					}
				}
			}
		})
	}
}

func TestDeserialize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType ValueType
		validate func(*testing.T, Value)
		wantErr  error
	}{
		{
			name:     "simple string",
			input:    "+hello world\r\n",
			wantType: ValueTypeSimpleString,
			validate: func(t *testing.T, v Value) {
				if string(v.Buffer) != "hello world" {
					t.Errorf("got %s, want hello world", string(v.Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:     "empty simple string",
			input:    "+\r\n",
			wantType: ValueTypeSimpleString,
			validate: func(t *testing.T, v Value) {
				if string(v.Buffer) != "" {
					t.Errorf("got %s, want empty string", string(v.Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:     "simple error with prefix and message",
			input:    "-ERR unknown command\r\n",
			wantType: ValueTypeSimpleError,
			validate: func(t *testing.T, v Value) {
				if string(v.SimpleErrorPrefix) != "ERR" {
					t.Errorf("prefix: got %s, want ERR", string(v.SimpleErrorPrefix))
				}
				if string(v.Buffer) != "unknown command" {
					t.Errorf("message: got %s, want unknown command", string(v.Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:     "simple error without message",
			input:    "-ERROR\r\n",
			wantType: ValueTypeSimpleError,
			validate: func(t *testing.T, v Value) {
				if string(v.SimpleErrorPrefix) != "ERROR" {
					t.Errorf("prefix: got %s, want ERROR", string(v.SimpleErrorPrefix))
				}
			},
			wantErr: nil,
		},
		{
			name:     "integer zero",
			input:    ":0\r\n",
			wantType: ValueTypeInteger,
			validate: func(t *testing.T, v Value) {
				if v.Integer != 0 {
					t.Errorf("got %d, want 0", v.Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:     "positive integer",
			input:    ":1000\r\n",
			wantType: ValueTypeInteger,
			validate: func(t *testing.T, v Value) {
				if v.Integer != 1000 {
					t.Errorf("got %d, want 1000", v.Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:     "negative integer",
			input:    ":-42\r\n",
			wantType: ValueTypeInteger,
			validate: func(t *testing.T, v Value) {
				if v.Integer != -42 {
					t.Errorf("got %d, want -42", v.Integer)
				}
			},
			wantErr: nil,
		},
		{
			name:     "bulk string",
			input:    "$11\r\nhello world\r\n",
			wantType: ValueTypeBulkString,
			validate: func(t *testing.T, v Value) {
				if string(v.Buffer) != "hello world" {
					t.Errorf("got %s, want hello world", string(v.Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:     "empty bulk string",
			input:    "$0\r\n\r\n",
			wantType: ValueTypeBulkString,
			validate: func(t *testing.T, v Value) {
				if string(v.Buffer) != "" {
					t.Errorf("got %s, want empty string", string(v.Buffer))
				}
			},
			wantErr: nil,
		},
		{
			name:     "null bulk string",
			input:    "$-1\r\n",
			wantType: ValueType(0),
			validate: func(t *testing.T, v Value) {
				if v.Buffer != nil {
					t.Errorf("got %v, want nil", v.Buffer)
				}
			},
			wantErr: nil,
		},
		{
			name:     "bulk string with binary data",
			input:    "$5\r\n\x00\x01\x02\x03\x04\r\n",
			wantType: ValueTypeBulkString,
			validate: func(t *testing.T, v Value) {
				if string(v.Buffer) != "\x00\x01\x02\x03\x04" {
					t.Errorf("got %v, want binary data", v.Buffer)
				}
			},
			wantErr: nil,
		},
		{
			name:     "array of integers",
			input:    "*3\r\n:1\r\n:2\r\n:3\r\n",
			wantType: ValueTypeArray,
			validate: func(t *testing.T, v Value) {
				if len(v.Array) != 3 {
					t.Errorf("length: got %d, want 3", len(v.Array))
				}
				for i, expected := range []int64{1, 2, 3} {
					if v.Array[i].Integer != expected {
						t.Errorf("element %d: got %d, want %d", i, v.Array[i].Integer, expected)
					}
				}
			},
			wantErr: nil,
		},
		{
			name:     "empty array",
			input:    "*0\r\n",
			wantType: ValueTypeArray,
			validate: func(t *testing.T, v Value) {
				if len(v.Array) != 0 {
					t.Errorf("length: got %d, want 0", len(v.Array))
				}
			},
			wantErr: nil,
		},
		{
			name:     "null array",
			input:    "*-1\r\n",
			wantType: ValueType(0),
			validate: func(t *testing.T, v Value) {
				if v.Array != nil {
					t.Errorf("got %v, want nil", v.Array)
				}
			},
			wantErr: nil,
		},
		{
			name:     "mixed array",
			input:    "*5\r\n+simple\r\n-ERR error\r\n:100\r\n$4\r\nbulk\r\n*2\r\n:1\r\n:2\r\n",
			wantType: ValueTypeArray,
			validate: func(t *testing.T, v Value) {
				if len(v.Array) != 5 {
					t.Errorf("length: got %d, want 5", len(v.Array))
				}
				if string(v.Array[0].Buffer) != "simple" {
					t.Errorf("element 0: got %s, want simple", string(v.Array[0].Buffer))
				}
				if v.Array[1].Type != ValueTypeSimpleError {
					t.Errorf("element 1: expected error type")
				}
				if v.Array[2].Integer != 100 {
					t.Errorf("element 2: got %d, want 100", v.Array[2].Integer)
				}
				if string(v.Array[3].Buffer) != "bulk" {
					t.Errorf("element 3: got %s, want bulk", string(v.Array[3].Buffer))
				}
				if len(v.Array[4].Array) != 2 {
					t.Errorf("element 4: nested array length got %d, want 2", len(v.Array[4].Array))
				}
			},
			wantErr: nil,
		},
		{
			name:     "unknown type",
			input:    "?unknown\r\n",
			wantType: ValueType(0),
			validate: nil,
			wantErr:  ErrUnknownValueType,
		},
		{
			name:     "invalid character",
			input:    "@invalid\r\n",
			wantType: ValueType(0),
			validate: nil,
			wantErr:  ErrUnknownValueType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := Deserialize(r)

			if err != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if got.Type != tt.wantType {
					t.Errorf("Deserialize() Type = %v, want %v", got.Type, tt.wantType)
				}
				if tt.validate != nil {
					tt.validate(t, got)
				}
			}
		})
	}
}

func TestDeserializeSimpleStringMalformed(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{
			name:    "missing CRLF",
			input:   "hello",
			wantErr: io.EOF,
		},
		{
			name:    "only CR",
			input:   "hello\r",
			wantErr: io.EOF,
		},
		{
			name:    "CR with wrong character",
			input:   "hello\rx",
			wantErr: ErrProtocolError,
		},
		{
			name:    "newline before CR",
			input:   "hello\nworld\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "multiple newlines",
			input:   "hello\n\nworld\r\n",
			wantErr: ErrProtocolError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			_, err := DeserializeSimpleString(r)
			if err != tt.wantErr {
				t.Errorf("DeserializeSimpleString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeserializeBulkStringMalformed(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{
			name:    "negative length other than -1",
			input:   "-5\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "length exceeds max size",
			input:   "1048577\r\n",
			wantErr: ErrTooLarge,
		},
		{
			name:    "invalid length format",
			input:   "abc\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "length with spaces",
			input:   "5 \r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "missing data",
			input:   "10\r\nhello",
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "missing final CRLF",
			input:   "5\r\nhello",
			wantErr: io.EOF,
		},
		{
			name:    "wrong character after CR",
			input:   "5\r\nhello\rx",
			wantErr: ErrProtocolError,
		},
		{
			name:    "only CR after data",
			input:   "5\r\nhello\r",
			wantErr: io.EOF,
		},
		{
			name:    "length mismatch - too short",
			input:   "10\r\nhello\r\n",
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "empty length",
			input:   "\r\n",
			wantErr: ErrProtocolError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			_, err := DeserializeBulkString(r)
			if err != tt.wantErr {
				t.Errorf("DeserializeBulkString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeserializeIntegerMalformed(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{
			name:    "empty input",
			input:   "\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "only minus sign",
			input:   "-\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "only plus sign",
			input:   "+\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "letters in number",
			input:   "12abc\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "special characters",
			input:   "12$34\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "space in number",
			input:   "12 34\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "multiple signs",
			input:   "--42\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "sign in middle",
			input:   "4-2\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "missing CR",
			input:   "42\n",
			wantErr: io.EOF,
		},
		{
			name:    "missing LF",
			input:   "42\r",
			wantErr: io.EOF,
		},
		{
			name:    "wrong character after CR",
			input:   "42\rabc",
			wantErr: ErrProtocolError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			_, err := DeserializeInteger(r)
			if err != tt.wantErr {
				t.Errorf("DeserializeInteger() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeserializeArrayMalformed(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{
			name:    "negative length other than -1",
			input:   "-5\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "invalid length format",
			input:   "abc\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "missing elements",
			input:   "3\r\n:1\r\n:2\r\n",
			wantErr: io.EOF,
		},
		{
			name:    "element with invalid type",
			input:   "2\r\n:1\r\n?invalid\r\n",
			wantErr: ErrUnknownValueType,
		},
		{
			name:    "truncated element",
			input:   "2\r\n:1\r\n+hello",
			wantErr: io.EOF,
		},
		{
			name:    "malformed integer element",
			input:   "2\r\n:abc\r\n:2\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "malformed bulk string element",
			input:   "2\r\n$5\r\nhello\r\n$-5\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "nested array with malformed element",
			input:   "2\r\n*2\r\n:1\r\n:abc\r\n:2\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "incomplete nested array",
			input:   "2\r\n*2\r\n:1\r\n:2\r\n",
			wantErr: io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			_, err := DeserializeArray(r)
			if err != tt.wantErr {
				t.Errorf("DeserializeArray() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeserializeMalformed(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{
			name:    "empty input",
			input:   "",
			wantErr: io.EOF,
		},
		{
			name:    "unknown type marker",
			input:   "!unknown\r\n",
			wantErr: ErrUnknownValueType,
		},
		{
			name:    "invalid simple string",
			input:   "+hello\nworld\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "invalid error",
			input:   "-ERR\ntest\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "invalid integer",
			input:   ":12a34\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "invalid bulk string length",
			input:   "$abc\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "bulk string too large",
			input:   "$2000000\r\n",
			wantErr: ErrTooLarge,
		},
		{
			name:    "invalid array length",
			input:   "*abc\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "array with invalid element",
			input:   "*2\r\n:1\r\n%invalid\r\n",
			wantErr: ErrUnknownValueType,
		},
		{
			name:    "nested array with malformed bulk string",
			input:   "*2\r\n*1\r\n$5\r\nabc\r\n:2\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "deeply nested invalid structure",
			input:   "*2\r\n*2\r\n*1\r\n:abc\r\n:2\r\n:3\r\n",
			wantErr: ErrProtocolError,
		},
		{
			name:    "mixed valid and invalid in array",
			input:   "*3\r\n+OK\r\n:42\r\n$-5\r\n",
			wantErr: ErrProtocolError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			_, err := Deserialize(r)
			if err != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
