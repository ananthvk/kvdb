package internal

import (
	"log/slog"
	"time"

	"github.com/ananthvk/kvdb"
	"github.com/spf13/afero"
)

// A wrapper around store, that also implements background compaction
// and periodic Sync

type KVStore struct {
	Path  string
	Store *kvdb.DataStore
}

func NewKVStore(datastorePath string) *KVStore {
	var fs afero.Fs
	if datastorePath == ":memory" {
		fs = afero.NewMemMapFs()
		datastorePath = "in-memory-" + time.Now().Format(time.RFC3339) + "-db"
	} else {
		fs = afero.NewOsFs()
	}

	start := time.Now()
	store, err := kvdb.Open(fs, datastorePath)
	if err != nil {
		slog.Error("open failed", "error", err)
		// Try creating it
		store, err = kvdb.Create(fs, datastorePath)
		if err != nil {
			slog.Error("create failed", "error", err)
			return nil
		}
		slog.Info("created datastore")
	}
	openDuration := time.Since(start)
	slog.Info("opened datastore", "path", datastorePath, "took", openDuration)
	return &KVStore{
		Path:  datastorePath,
		Store: store,
	}
}

func (kv *KVStore) Close() error {
	if kv.Store != nil {
		slog.Info("closing store", "path", kv.Path)
		return kv.Store.Close()
	}
	return nil
}
