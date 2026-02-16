package resp

type ValueType int

const (
	ValueTypeNil ValueType = iota
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
