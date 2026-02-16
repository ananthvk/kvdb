package internal

import "github.com/ananthvk/kvdb/internal/resp"

type CommandFunc func(args []resp.Value, store *KVStore) resp.Value

var Commands = map[string]CommandFunc{
	"ECHO": handleEcho,
	"PING": handlePing,
}
