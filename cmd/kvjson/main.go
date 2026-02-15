package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/ananthvk/kvdb"
	"github.com/spf13/afero"
)

// UserProfile mimics a real-world document
type UserProfile struct {
	ID       string            `json:"id"`
	Username string            `json:"username"`
	Email    string            `json:"email"`
	IsActive bool              `json:"is_active"`
	Age      int               `json:"age"`
	Tags     []string          `json:"tags"`
	Metadata map[string]string `json:"metadata"`
	// Payload is used to pad the record to a specific size
	Payload string `json:"payload,omitempty"`
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func generateJSON(targetSize int) ([]byte, string) {
	// 1. Generate a Key
	key := fmt.Sprintf("user:%s", randomString(16))

	// 2. Build the Object
	user := UserProfile{
		ID:       key,
		Username: randomString(8),
		Email:    fmt.Sprintf("%s@example.com", randomString(8)),
		IsActive: rand.Intn(2) == 1,
		Age:      rand.Intn(60) + 18,
		Tags:     []string{"developer", "golang", "db-engine", "benchmark"},
		Metadata: map[string]string{
			"login_ip": "192.168.1.1",
			"device":   "MacBook Pro",
		},
	}

	// 3. Calculate Padding needed
	// Marshal once to see base size
	baseBytes, _ := json.Marshal(user)
	baseSize := len(baseBytes)

	if targetSize > baseSize {
		paddingNeeded := targetSize - baseSize
		user.Payload = randomString(paddingNeeded)
	}

	// 4. Final Marshal
	finalBytes, _ := json.Marshal(user)
	return finalBytes, key
}

func main() {
	numOps := flag.Int("n", 10000, "Total number of records to write")
	targetSize := flag.Int("size", 1024, "Target size of JSON value in bytes (default 1KB)")
	dbPath := flag.String("db", "./mydb", "Path to KVDB database")
	flag.Parse()

	fmt.Printf("Generating %d JSON records (Size: ~%d bytes each)...\n", *numOps, *targetSize)

	fs := afero.NewOsFs()

	// Open or Create DB
	store, err := kvdb.Open(fs, *dbPath)
	if err != nil {
		fmt.Println("Database not found, creating new one...")
		store, err = kvdb.Create(fs, *dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
			os.Exit(1)
		}
	}
	defer store.Close()

	start := time.Now()

	// Batch Loop
	for i := 0; i < *numOps; i++ {
		valBytes, keyStr := generateJSON(*targetSize)

		if err := store.Put([]byte(keyStr), valBytes); err != nil {
			fmt.Fprintf(os.Stderr, "Write error: %v\n", err)
			continue
		}

		if i%1000 == 0 && i > 0 {
			fmt.Printf("\rWrote %d/%d records...", i, *numOps)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\nDone! Wrote %d records in %s\n", *numOps, elapsed)
	fmt.Printf("Throughput: %.2f records/sec\n", float64(*numOps)/elapsed.Seconds())
}
