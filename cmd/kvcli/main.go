package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/ananthvk/kvdb"
	"github.com/spf13/afero"
)

func main() {
	fs := afero.NewOsFs()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: kvcli <path>")
		os.Exit(1)
	}
	store, err := kvdb.Open(fs, os.Args[1])
	if err != nil {
		fmt.Println(err)
		// Try creating it
		store, err = kvdb.Create(fs, os.Args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "(error) CREATE: %s", err)
			os.Exit(1)
		}
	}
	defer store.Close()

	fmt.Println("Welcome to kvdb cli, type \"exit\" to quit")
	// TODO: NOTE: Cannot set/get a key called \key, introduce escape sequence or quotes "" to avoid this
	fmt.Println("To set a value, use <key>=<value>, to retrieve a value just type <key>, to get all keys type \\keys, to delete a key \\delete <key>")
	fmt.Println("Note: Spaces matter, so key =value is different from key=value")
	fmt.Print("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		query := scanner.Text()
		if query == "exit" {
			break
		}
		var output string
		switch query {
		case "":
			continue
		case "\\keys":
			keys, err := store.ListKeys()
			if err != nil {
				fmt.Printf("(error) \\keys: %s", err)
				continue
			}
			stringKeys := make([]string, len(keys))
			for i, key := range keys {
				stringKeys[i] = string(key)
			}
			output = "[" + strings.Join(stringKeys, ",") + "]"
		case "\\size":
			output = fmt.Sprintf("%d", store.Size())
		case "\\sync":
			store.Sync()
			output = "OK"
		case "\\seed":
			for i := 1; i <= 15; i++ {
				err := store.Put(fmt.Appendf(nil, "key%02d", i), fmt.Appendf(nil, "value%d", i))
				if err != nil {
					output = fmt.Sprintf("(error) SEED: %s", err)
					break
				}
			}
			for i := 1; i <= 10; i++ {
				err := store.Delete(fmt.Appendf(nil, "key%02d", i))
				if err != nil {
					output = fmt.Sprintf("(error) DELETE: %s", err)
					break
				}
			}
			for i := 1; i <= 5; i++ {
				err := store.Put(fmt.Appendf(nil, "key%02d", i), fmt.Appendf(nil, "updated_value%d", i))
				if err != nil {
					output = fmt.Sprintf("(error) UPDATE: %s", err)
					break
				}
			}
			for i := 12; i <= 13; i++ {
				err := store.Put(fmt.Appendf(nil, "key%02d", i), fmt.Appendf(nil, "updated_value%d", i))
				if err != nil {
					output = fmt.Sprintf("(error) UPDATE: %s", err)
					break
				}
			}
			for i := 16; i <= 17; i++ {
				err := store.Put(fmt.Appendf(nil, "key%02d", i), fmt.Appendf(nil, "value%d", i))
				if err != nil {
					output = fmt.Sprintf("(error) ADD: %s", err)
					break
				}
			}
			output = "Seeding completed"
		case "\\merge":
			go func() {
				err := store.Merge()
				if err == nil {
					fmt.Println("MERGE OK")
				} else {
					fmt.Println("MERGE ERR", err)
				}
			}()
			output = "PENDING"
		case "\\scan":
			keys, err := store.ListKeys()
			if err != nil {
				output = fmt.Sprintf("(error) \\scan: %s", err)
				break
			}
			sort.Strings(keys) // Sort the keys
			var values []string
			for _, key := range keys {
				value, err := store.Get([]byte(key))
				if err != nil {
					values = append(values, fmt.Sprintf("(error) GET %s: %s", key, err))
				} else {
					values = append(values, fmt.Sprintf("%s=%s", key, value))
				}
			}
			output = strings.Join(values, "\n")
		default:
			if after, ok := strings.CutPrefix(query, "\\delete "); ok {
				key := after
				err := store.Delete([]byte(key))
				if err != nil {
					output = fmt.Sprintf("(error) DELETE: %s", err)
				} else {
					output = "OK"
				}
				fmt.Println(output)
				fmt.Print("> ")
				continue
			}
			before, after, found := strings.Cut(query, "=")
			if found {
				// A SET operation
				err := store.Put([]byte(before), []byte(after))
				if err != nil {
					output = fmt.Sprintf("(error) SET: %s", err)
				} else {
					output = "OK"
				}
			} else {
				// A GET operation
				op, err := store.Get([]byte(before))
				if err != nil {
					output = fmt.Sprintf("(error) GET: %s", err)
				} else {
					output = string(op)
				}
			}
		}
		fmt.Println(output)
		fmt.Print("> ")
	}
}
