package kvdb

import (
	"testing"

	"github.com/spf13/afero"
)

func BenchmarkRead(b *testing.B) {
	testFS := afero.NewMemMapFs()
	store, err := Create(testFS, "test1.dat")
	if err != nil {
		b.Fatalf("could not create datastore %v", err)
	}
	key := []byte("small key")
	store.Put(key, []byte("The quick brown fox jumps over the lazy dogs"))
	for b.Loop() {
		store.Get(key)
	}
}

func BenchmarkWriteLargeData(b *testing.B) {
	testFS := afero.NewMemMapFs()
	store, err := Create(testFS, "test_write.dat")
	if err != nil {
		b.Fatalf("could not create datastore %v", err)
	}

	// Pre-allocate key and value buffers
	key := make([]byte, 999)       // 1 KB key
	value := make([]byte, 999*999) // 1 MB value

	// Fill with some data
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	i := 0
	for b.Loop() {
		// Vary the key slightly for each iteration
		key[0] = byte(i % 256)
		if err := store.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
		i++
	}
}

func BenchmarkReadLargeData(b *testing.B) {
	testFS := afero.NewMemMapFs()
	store, err := Create(testFS, "test_read.dat")
	if err != nil {
		b.Fatalf("could not create datastore %v", err)
	}

	// Pre-allocate key and value buffers
	key := make([]byte, 999)       // 1 KB key
	value := make([]byte, 999*999) // 1 MB value

	// Fill with some data
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte(i % 256)
	}

	// Write the data once before benchmarking reads
	if err := store.Put(key, value); err != nil {
		b.Fatalf("Put failed: %v", err)
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := store.Get(key); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}
