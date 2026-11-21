package live

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/ankojh/dor/internal/pb/dorv1"
	"google.golang.org/protobuf/proto"
)

// MarshalSnapshot encodes the KV cache. Large globes exceed NATS' default 1MiB
// payload; compress storage while keeping WebSocket messages ordinary protobuf.
func MarshalSnapshot(snapshot *dorv1.Snapshot) ([]byte, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil || len(data) < 512*1024 {
		return data, err
	}
	var out bytes.Buffer
	writer, err := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// UnmarshalSnapshot accepts both the original protobuf and compressed cache.
func UnmarshalSnapshot(data []byte, snapshot *dorv1.Snapshot) error {
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return err
		}
		defer reader.Close()
		// Bound corrupt or hostile compressed cache values before allocating them.
		data, err = io.ReadAll(io.LimitReader(reader, 64*1024*1024+1))
		if err != nil {
			return err
		}
		if len(data) > 64*1024*1024 {
			return fmt.Errorf("live: snapshot exceeds 64MiB")
		}
	}
	return proto.Unmarshal(data, snapshot)
}
