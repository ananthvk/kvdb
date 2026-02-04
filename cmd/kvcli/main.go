package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	kvdb "github.com/ananthvk/kvdb"
)

func main() {
	store, err := kvdb.NewDataStore(":memory")
	if err != nil {
		fmt.Fprintf(os.Stderr, "(error) OPEN: %s", err)
		os.Exit(1)
	}
	defer store.Close()

	fmt.Println("Welcome to kvdb cli, type \"exit\" to quit")
	// TODO: NOTE: Cannot set/get a key called \key, introduce escape sequence or quotes "" to avoid this
	fmt.Println("To set a value, use <key>=<value>, to retrieve a value just type <key>, to get all keys type \\keys")
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
			op, err := store.ListKeys()
			if err != nil {
				fmt.Printf("(error) \\keys: %s", err)
				continue
			}
			output = "[" + strings.Join(op, ",") + "]"
		case "\\size":
			output = fmt.Sprintf("%d", store.Size())
		default:
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
