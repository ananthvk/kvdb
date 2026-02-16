package internal

import (
	"errors"

	"github.com/ananthvk/kvdb"
	"github.com/ananthvk/kvdb/internal/resp"
)

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

func handleGet(args []resp.Value, store *KVStore) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("ERR"),
			Buffer:            []byte("wrong number of arguments for 'GET' command"),
		}
	}
	value, err := store.Store.Get(args[0].Buffer)
	if err != nil {
		if errors.Is(err, kvdb.ErrKeyNotFound) {
			return resp.Value{Type: resp.ValueTypeNull}
		}
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("INTERNAL_ERR"),
			Buffer:            []byte(err.Error()),
		}
	}
	return resp.Value{
		Type:   resp.ValueTypeBulkString,
		Buffer: value,
	}
}

func handleSet(args []resp.Value, store *KVStore) resp.Value {
	if len(args) != 2 {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("ERR"),
			Buffer:            []byte("wrong number of arguments for 'SET' command"),
		}
	}

	if err := store.Store.Put(args[0].Buffer, args[1].Buffer); err != nil {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("INTERNAL_ERR"),
			Buffer:            []byte(err.Error()),
		}
	}

	return resp.Value{
		Type:   resp.ValueTypeSimpleString,
		Buffer: []byte{'O', 'K'},
	}
}
