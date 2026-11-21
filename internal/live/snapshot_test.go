package live

import (
	"bytes"
	"testing"

	"github.com/ankojh/dor/internal/pb/dorv1"
	"google.golang.org/protobuf/proto"
)

func largeSnapshot() *dorv1.Snapshot {
	s := &dorv1.Snapshot{Seq: 812, Stories: make([]*dorv1.Story, 4645), Links: make([]*dorv1.Link, 13302)}
	for i := range s.Stories {
		s.Stories[i] = &dorv1.Story{Id: stringID("story", i), Title: "Representative story title with enough descriptive detail to model the production globe and preserve realistic place, source, category and timestamp metadata"}
	}
	for i := range s.Links {
		s.Links[i] = &dorv1.Link{A: stringID("story", i%4645), B: stringID("story", (i+1)%4645), Strength: .6, Confidence: .8}
	}
	return s
}

func stringID(prefix string, i int) string {
	const digits = "0123456789"
	if i == 0 {
		return prefix + "-0"
	}
	var buf [20]byte
	at := len(buf)
	for i > 0 {
		at--
		buf[at] = digits[i%10]
		i /= 10
	}
	return prefix + "-" + string(buf[at:])
}

func TestSnapshotCodecCompressesAndRoundTripsLargeGlobe(t *testing.T) {
	want := largeSnapshot()
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 1024*1024 {
		t.Fatalf("fixture protobuf is only %d bytes; expected to exceed the KV payload limit", len(raw))
	}
	encoded, err := MarshalSnapshot(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= len(raw) || len(encoded) >= 1024*1024 {
		t.Fatalf("encoded size=%d raw=%d, want compressed and under 1MiB", len(encoded), len(raw))
	}
	if !bytes.HasPrefix(encoded, []byte{0x1f, 0x8b}) {
		t.Fatal("large snapshot was not gzip encoded")
	}
	var got dorv1.Snapshot
	if err := UnmarshalSnapshot(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &got) {
		t.Fatal("large snapshot round trip changed data")
	}
}

func TestSnapshotCodecReadsLegacyRawProtobufAndRejectsCorruption(t *testing.T) {
	want := &dorv1.Snapshot{Seq: 9, Stories: []*dorv1.Story{{Id: "legacy"}}}
	raw, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got dorv1.Snapshot
	if err := UnmarshalSnapshot(raw, &got); err != nil {
		t.Fatalf("legacy protobuf: %v", err)
	}
	if !proto.Equal(want, &got) {
		t.Fatal("legacy snapshot changed")
	}
	for _, data := range [][]byte{{0x1f, 0x8b}, {0x1f, 0x8b, 0, 1, 2, 3}} {
		if err := UnmarshalSnapshot(data, &dorv1.Snapshot{}); err == nil {
			t.Fatalf("corrupt compressed bytes accepted: %x", data)
		}
	}
}

func TestSnapshotCodecLeavesSmallProtobufUncompressed(t *testing.T) {
	s := &dorv1.Snapshot{Seq: 4, Stories: []*dorv1.Story{{Id: "small"}}}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, raw) {
		t.Fatal("small snapshot storage encoding changed")
	}
}
