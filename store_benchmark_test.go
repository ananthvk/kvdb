package kvdb

import (
	"fmt"
	"testing"
)

func BenchmarkRead(b *testing.B) {
	store, err := NewDataStore(":memory")
	key := []byte("small key")
	store.Put(key, []byte("The quick brown fox jumps over the lazy dogs"))
	if err != nil {
		b.Fatalf("error: could not create database: %s", err)
	}
	for b.Loop() {
		store.Get(key)
	}
}

func BenchmarkFold(b *testing.B) {
	store, err := NewDataStore(":memory")
	count := 1000
	// Insert count keys
	keys := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "%d", i)
		store.Put(keys[i], []byte("ha ha"))
	}
	if err != nil {
		b.Fatalf("error: could not create database: %s", err)
	}
	for b.Loop() {
		acc, _ := Fold(store, func(k, v []byte, acc int) int { return acc + 1 }, 0)
		if acc != count {
			b.Fatalf("expected 1000 items, got %d", acc)
		}
	}
}
