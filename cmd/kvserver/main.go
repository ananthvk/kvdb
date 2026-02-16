package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/ananthvk/kvdb/cmd/kvserver/internal"
)

func main() {
	portPtr := flag.Uint("port", 6379, "specify the port on which to listen")
	hostPtr := flag.String("host", "0.0.0.0", "specify the bind address")
	dbPtr := flag.String("db", "", "specify the datastore directory path")
	flag.Parse()
	if *dbPtr == "" {
		slog.Error("database directory path is required")
		return
	}
	address := fmt.Sprintf("%s:%d", *hostPtr, *portPtr)

	ctx := context.Background()
	listenerConfig := net.ListenConfig{}
	listener, err := listenerConfig.Listen(ctx, "tcp", address)
	if err != nil {
		slog.Error("listen failed", "error", err)
		return
	}
	store := internal.NewKVStore(*dbPtr)
	if store == nil {
		slog.Error("datastore could not be openend, exiting")
		os.Exit(1)
	}
	defer store.Close()
	slog.Info("server listening", "address", listener.Addr().String(), "datastore", store.Path)
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.Warn("accept failed", "error", err)
			continue
		}
		go store.Handle(conn)
	}
}
