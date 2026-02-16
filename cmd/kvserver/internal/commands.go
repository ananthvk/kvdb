package internal

import "github.com/ananthvk/kvdb/internal/resp"

func handleEcho(args []resp.Value, store *KVStore) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("ERR"),
			Buffer:            []byte("wrong number of arguments for 'ECHO' command"),
		}
	}

	return resp.Value{
		Type:   resp.ValueTypeBulkString,
		Buffer: args[0].Buffer,
	}
}

func handlePing(args []resp.Value, store *KVStore) resp.Value {
	switch len(args) {
	case 0:
		return resp.Value{
			Type:   resp.ValueTypeSimpleString,
			Buffer: []byte("PONG"),
		}
	case 1:
		return resp.Value{
			Type:   resp.ValueTypeBulkString,
			Buffer: args[0].Buffer,
		}
	default:
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("ERR"),
			Buffer:            []byte("wrong number of arguments for 'PING' command"),
		}
	}
}
