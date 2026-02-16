package internal

import (
	"errors"
	"sort"

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

// Pattern is ignored though (for now, KEYS means KEYS *)
func handleKeys(args []resp.Value, store *KVStore) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("ERR"),
			Buffer:            []byte("wrong number of arguments for 'KEYS' command"),
		}
	}
	keys, err := store.Store.ListKeys()
	if err != nil {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("INTERNAL_ERR"),
			Buffer:            []byte(err.Error()),
		}
	}
	sort.Strings(keys) // Sort the keys

	values := make([]resp.Value, len(keys))
	for i, key := range keys {
		values[i] = resp.Value{
			Type:   resp.ValueTypeBulkString,
			Buffer: []byte(key),
		}
	}
	return resp.Value{
		Type:  resp.ValueTypeArray,
		Array: values,
	}
}

func handleDel(args []resp.Value, store *KVStore) resp.Value {
	if len(args) == 0 {
		return resp.Value{
			Type:              resp.ValueTypeSimpleError,
			SimpleErrorPrefix: []byte("ERR"),
			Buffer:            []byte("wrong number of arguments for 'GET' command"),
		}
	}
	deleteCount := 0
	for _, key := range args {
		keyExisted, err := store.Store.DeleteWithExists(key.Buffer)
		if err != nil {
			return resp.Value{
				Type:              resp.ValueTypeSimpleError,
				SimpleErrorPrefix: []byte("INTERNAL_ERR"),
				Buffer:            []byte(err.Error()),
			}
		}
		if keyExisted {
			deleteCount++
		}
	}
	return resp.Value{
		Type:    resp.ValueTypeInteger,
		Integer: int64(deleteCount),
	}
}
