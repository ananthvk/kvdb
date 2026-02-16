package resp

type ValueType int

const maxBulkStringSize = 1024 * 1024 // 1 MiB

const (
	ValueTypeNull ValueType = iota
	ValueTypeSimpleString
	ValueTypeSimpleError
	ValueTypeInteger
	ValueTypeBulkString
	ValueTypeArray
)

type Value struct {
	Type              ValueType
	SimpleErrorPrefix []byte
	Buffer            []byte
	Array             []Value
	Integer           int64
}
