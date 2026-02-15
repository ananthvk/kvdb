package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/ananthvk/kvdb"
	"github.com/spf13/afero"
)

func randomBytes(length int) []byte {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return b
}

func main() {
	num := flag.Int("n", 10000, "Total number of operations")
	flag.Parse()
	fmt.Println("total ops ", *num)

	fs := afero.NewOsFs()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: kvmake <path>")
		os.Exit(1)
	}

	store, err := kvdb.Open(fs, flag.Args()[0])
	if err != nil {
		fmt.Println(err)
		// Try creating it
		fmt.Printf("creating it...\n")
		store, err = kvdb.Create(fs, flag.Args()[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "(error) CREATE: %s", err)
			os.Exit(1)
		}
	}
	defer store.Close()

	for i := 0; i < *num; i++ {
		key := randomBytes(rand.Intn(30) + 15)
		value := randomBytes(rand.Intn(20) + 10)
		if err := store.Put(key, value); err != nil {
			fmt.Fprintf(flag.CommandLine.Output(), "write error: %v\n", err)
		}
	}
}
