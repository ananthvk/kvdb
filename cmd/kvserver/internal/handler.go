package internal

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/ananthvk/kvdb/internal/resp"
)

func sendResponse(value resp.Value, writer *bufio.Writer) error {
	err := resp.Serialize(value, writer)
	if err == nil {
		return writer.Flush()
	} else {
		slog.Error("error serializing response", "err", err)
	}
	return err
}

func sendRequestError(message []byte, writer *bufio.Writer) error {
	return sendResponse(resp.Value{
		Type:              resp.ValueTypeSimpleError,
		SimpleErrorPrefix: []byte("REQUEST_ERR"),
		Buffer:            message,
	}, writer)
}

func sendError(message []byte, writer *bufio.Writer) error {
	return sendResponse(resp.Value{
		Type:              resp.ValueTypeSimpleError,
		SimpleErrorPrefix: []byte("ERR"),
		Buffer:            message,
	}, writer)
}

func (kvStore *KVStore) Handle(conn net.Conn) {
	slog.Info("client connected", "remote_address", conn.RemoteAddr().String())
	defer func() {
		slog.Info("client disconnected", "remote_address", conn.RemoteAddr().String())
	}()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Process requests
	for {
		req, err := resp.Deserialize(reader)
		if err != nil {
			if errors.Is(err, resp.ErrProtocolError) {
				sendRequestError([]byte(err.Error()), writer)
			}
			break
		}

		if req.Type != resp.ValueTypeArray || len(req.Array) == 0 {
			sendRequestError([]byte("invalid request: request must be an array of bulk strings"), writer)
			continue
		}

		shouldSkip := false
		for _, value := range req.Array {
			if value.Type != resp.ValueTypeBulkString {
				sendRequestError([]byte("invalid request: all array elements must be bulk strings"), writer)
				shouldSkip = true
				break
			}
		}
		if shouldSkip {
			continue
		}

		commandRootName := bytes.ToUpper(req.Array[0].Buffer)
		commandFunc, exists := Commands[string(commandRootName)]
		if !exists {
			sendError(fmt.Appendf(nil, "%s '%s'", "unknown command", req.Array[0].Buffer), writer)
			continue
		}
		result := commandFunc(req.Array[1:], kvStore)
		if err := sendResponse(result, writer); err != nil {
			break
		}
	}
}
