package resp

import (
	"bufio"
	"bytes"
	"strconv"
)

func SerializeSimpleString(buf []byte, w *bufio.Writer) error {
	if bytes.ContainsAny(buf, "\r\n") {
		return ErrInvalidValue
	}
	if _, err := w.Write([]byte{'+'}); err != nil {
		return err
	}

	if _, err := w.Write(buf); err != nil {
		return err
	}

	_, err := w.Write([]byte("\r\n"))
	return err
}

func SerializeSimpleError(prefix []byte, content []byte, w *bufio.Writer) error {
	if bytes.ContainsAny(prefix, "\r\n") {
		return ErrInvalidValue
	}
	if bytes.ContainsAny(content, "\r\n") {
		return ErrInvalidValue
	}

	if _, err := w.Write([]byte{'-'}); err != nil {
		return err
	}

	if _, err := w.Write(prefix); err != nil {
		return err
	}
	if len(prefix) > 0 {
		// Add a space separator
		if _, err := w.Write([]byte{' '}); err != nil {
			return err
		}
	}
	if _, err := w.Write(content); err != nil {
		return err
	}

	_, err := w.Write([]byte("\r\n"))
	return err
}

func SerializeInteger(value int64, w *bufio.Writer) error {
	if _, err := w.Write([]byte{':'}); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.FormatInt(value, 10)); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}
	return nil
}

func SerializeBulkString(buf []byte, w *bufio.Writer) error {
	if _, err := w.Write([]byte{'$'}); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.Itoa(len(buf))); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}
	return nil
}

func SerializeArray(values []Value, w *bufio.Writer) error {
	if _, err := w.Write([]byte{'*'}); err != nil {
		return err
	}
	if _, err := w.WriteString(strconv.Itoa(len(values))); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\r\n")); err != nil {
		return err
	}
	for _, v := range values {
		if err := Serialize(v, w); err != nil {
			return err
		}
	}
	return nil
}

func SerializeNull(w *bufio.Writer) error {
	_, err := w.Write([]byte("_\r\n"))
	return err
}

func Serialize(value Value, w *bufio.Writer) error {
	switch value.Type {
	case ValueTypeNull:
		return SerializeNull(w)
	case ValueTypeSimpleString:
		return SerializeSimpleString(value.Buffer, w)
	case ValueTypeSimpleError:
		return SerializeSimpleError(value.SimpleErrorPrefix, value.Buffer, w)
	case ValueTypeInteger:
		return SerializeInteger(value.Integer, w)
	case ValueTypeBulkString:
		return SerializeBulkString(value.Buffer, w)
	case ValueTypeArray:
		return SerializeArray(value.Array, w)
	}
	return ErrInvalidType
}
