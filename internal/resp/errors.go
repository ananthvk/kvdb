package resp

import (
	"errors"
	"fmt"
)

var ErrProtocolError = errors.New("protocol error")

var ErrTooLarge = fmt.Errorf("%w: bulk string length too large", ErrProtocolError)

var ErrUnknownValueType = fmt.Errorf("%w: unknown value type", ErrProtocolError)

var ErrInvalidType = fmt.Errorf("%w: invalid value of Type during serialization", ErrProtocolError)

var ErrInvalidValue = fmt.Errorf("%w: resp serialization: invalid value", ErrProtocolError)
