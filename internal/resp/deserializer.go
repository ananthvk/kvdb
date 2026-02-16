package resp

import (
	"bufio"
	"io"
	"slices"
)

// All these deserialize functions must be called depending upon the type, i.e. after parsing the
// first byte of the input

// DeserializeSimpleString reads a simple string from the reader. This function should be called after the '+' character
// has been processed.
func DeserializeSimpleString(r *bufio.Reader) (Value, error) {
	// Read upto \r
	simpleString, err := r.ReadBytes('\r')
	if err != nil {
		return Value{}, err
	}
	if slices.Contains(simpleString, '\n') {
		// \n should not come before \r
		return Value{}, ErrProtocolError
	}

	// Check if the next byte is \n
	nextByte, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	if nextByte != '\n' {
		return Value{}, ErrProtocolError
	}
	// Remove the trailing \r character
	buf := simpleString[:len(simpleString)-1]
	return Value{
		Type:   ValueTypeSimpleString,
		Buffer: buf,
	}, nil
}

// DeserializeError should be called after '-' byte has been processed.
// It returns the error prefix, i.e. the first word after the '-' upto newline or space
// And the message
func DeserializeError(r *bufio.Reader) (Value, error) {
	value, err := DeserializeSimpleString(r)
	if err != nil {
		return Value{}, err
	}
	simpleString := value.Buffer
	spaceIdx := slices.Index(simpleString, ' ')
	if spaceIdx == -1 {
		// No space found, entire string is the prefix
		return Value{
			Type:              ValueTypeSimpleError,
			SimpleErrorPrefix: simpleString,
			Buffer:            simpleString,
		}, nil
	}
	prefix := simpleString[:spaceIdx]
	message := simpleString[spaceIdx+1:]
	return Value{
		Type:              ValueTypeSimpleError,
		SimpleErrorPrefix: prefix,
		Buffer:            message,
	}, nil
}

// DeserializeBulkString should be called after '$' byte has been processed.
// It can process any kind of binary strings
func DeserializeBulkString(r *bufio.Reader) (Value, error) {
	value, err := DeserializeInteger(r)
	if err != nil {
		return value, err
	}
	length := value.Integer
	// Handle null bulk string
	if length == -1 {
		return Value{}, nil
	}

	if length < 0 {
		return Value{}, ErrProtocolError
	}

	if length > maxBulkStringSize {
		return Value{}, ErrTooLarge
	}

	// Read the data
	data := make([]byte, length)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return Value{}, err
	}

	// Read and verify \r\n
	if err := checkCLRF(r); err != nil {
		return Value{}, err
	}

	return Value{
		Type:   ValueTypeBulkString,
		Buffer: data,
	}, nil
}

// DeserializeArray deserializes an arbitrary array from the reader. Each element is parsed as a RESP value
// It should be called after '*' has been processed
func DeserializeArray(r *bufio.Reader) (Value, error) {
	value, err := DeserializeInteger(r)
	if err != nil {
		return value, err
	}
	length := value.Integer

	// Handle null array
	if length == -1 {
		return Value{}, nil
	}
	if length < 0 {
		return Value{}, ErrProtocolError
	}

	values := make([]Value, length)

	// Read the values
	for i := range values {
		value, err := Deserialize(r)
		if err != nil {
			return Value{}, err
		}
		values[i] = value
	}

	return Value{
		Type:  ValueTypeArray,
		Array: values,
	}, nil
}

// Deserialize is a high level function that reads the first byte to determine the type of value.
// It then calls the appropriate function to deserialize the value
func Deserialize(r *bufio.Reader) (Value, error) {
	valueTypeByte, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch valueTypeByte {
	case '+':
		return DeserializeSimpleString(r)
	case '-':
		return DeserializeError(r)
	case ':':
		return DeserializeInteger(r)
	case '$':
		return DeserializeBulkString(r)
	case '*':
		return DeserializeArray(r)
	case '_':
		return DeserializeNull(r)
	}
	return Value{}, ErrUnknownValueType
}

// DeserializeNull deserializes a null value. It should be called after '_' has been processed.
// It verifies that the next bytes are \r\n
func DeserializeNull(r *bufio.Reader) (Value, error) {
	if err := checkCLRF(r); err != nil {
		return Value{}, err
	}
	return Value{Type: ValueTypeNull}, nil
}

// DeserializeInteger deserializes a signed 64-bit signed integer. It should be called after ':' has been processed
func DeserializeInteger(r *bufio.Reader) (Value, error) {
	// Read the length integer
	lengthBytes, err := r.ReadBytes('\r')
	if err != nil {
		return Value{}, err
	}
	// Check if the next byte is \n
	nextByte, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	if nextByte != '\n' {
		return Value{}, ErrProtocolError
	}

	numDigits := 0

	// Parse the length (excluding the trailing \r)
	var length int64 = 0
	isNegative := false
	for i, b := range lengthBytes[:len(lengthBytes)-1] {
		if i == 0 && b == '-' {
			isNegative = true
			continue
		}
		if i == 0 && b == '+' {
			// Optional + sign
			continue
		}
		if b < '0' || b > '9' {
			return Value{}, ErrProtocolError
		}
		numDigits += 1
		length = length*10 + int64(b-'0')
	}
	if isNegative {
		length = -length
	}

	// There should be atleast a single digit
	if numDigits == 0 {
		return Value{}, ErrProtocolError
	}
	return Value{Type: ValueTypeInteger, Integer: length}, nil
}

func checkCLRF(r *bufio.Reader) error {
	crlfByte, err := r.ReadByte()
	if err != nil {
		return err
	}
	if crlfByte != '\r' {
		return ErrProtocolError
	}

	lfByte, err := r.ReadByte()
	if err != nil {
		return err
	}
	if lfByte != '\n' {
		return ErrProtocolError
	}
	return nil
}
