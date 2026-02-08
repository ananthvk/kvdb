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

/*
func BenchmarkFold(b *testing.B) {
	testFS := afero.NewMemMapFs()
	store, err := Create(testFS, "test2.dat")
	if err != nil {
		b.Fatalf("could not create datastore %v", err)
	}
	count := 1000
	// Insert count keys
	keys := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "%d", i)
		store.Put(keys[i], []byte("ha ha"))
	}
	for b.Loop() {
		acc, _ := Fold(store, func(k, v []byte, acc int) int { return acc + 1 }, 0)
		if acc != count {
			b.Fatalf("expected 1000 items, got %d", acc)
		}
	}
}
*/
