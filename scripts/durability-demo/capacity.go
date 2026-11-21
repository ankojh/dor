package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"google.golang.org/protobuf/proto"
)

// capacityReport extends the copied rows to the planned presentation load. This
// measures representative content; random/high-entropy data can still be larger.
func capacityReport(b *bus.Bus, original *dorv1.Snapshot) error {
	s := proto.Clone(original).(*dorv1.Snapshot)
	if len(s.Stories) == 0 || len(s.Links) == 0 {
		return fmt.Errorf("capacity fixture needs copied stories and links")
	}
	for len(s.Stories) < 5000 {
		i := len(s.Stories)
		story := proto.Clone(s.Stories[i%len(original.Stories)]).(*dorv1.Story)
		story.Id = fmt.Sprintf("capacity-story-%d", i)
		s.Stories = append(s.Stories, story)
	}
	seen := make(map[string]bool)
	for _, l := range s.Links {
		seen[l.A+"|"+l.B] = true
	}
	for i := 0; len(s.Links) < 20000; i++ {
		a, b := s.Stories[i%5000].Id, s.Stories[(i/5000+i+19)%5000].Id
		if a > b {
			a, b = b, a
		}
		if a == b || seen[a+"|"+b] {
			continue
		}
		seen[a+"|"+b] = true
		l := proto.Clone(original.Links[i%len(original.Links)]).(*dorv1.Link)
		l.A, l.B = a, b
		s.Links = append(s.Links, l)
	}
	raw, err := proto.Marshal(s)
	if err != nil {
		return err
	}
	stored, err := live.MarshalSnapshot(s)
	if err != nil {
		return err
	}
	revisions := make(map[string]uint64)
	for _, story := range s.Stories {
		revisions["STORY/story:"+story.Id] = 12345
	}
	for _, link := range s.Links {
		revisions["STORY/link:"+link.A+"|"+link.B] = 54321
	}
	scores := make(map[string]*dorv1.MatchScore)
	for _, row := range s.RecentWire {
		for _, stage := range []string{"clean:", "placed:", "score:"} {
			revisions["ENRICH/"+stage+row.ArticleId] = 12345
		}
		scores[row.ArticleId] = &dorv1.MatchScore{ArticleId: row.ArticleId, Scores: map[string]float32{"capacity-prompt": .8}}
	}
	record := map[string]any{"snapshot": stored, "stream_seq": 1, "revisions": revisions, "scores": scores, "owner": "", "lease_until": 0}
	encode := func(value any) ([]byte, error) {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		var out bytes.Buffer
		w, err := gzip.NewWriterLevel(&out, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(data); err != nil {
			return nil, err
		}
		if err = w.Close(); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}
	checkpoint, err := encode(record)
	if err != nil {
		return err
	}
	rootBytes := len(checkpoint)
	chunks := 0
	if int64(rootBytes)+1024 > b.NC().MaxPayload() {
		hash := fmt.Sprintf("%x", sha256.Sum256(checkpoint))
		var keys []string
		for offset := 0; offset < len(checkpoint); offset += 256 * 1024 {
			keys = append(keys, fmt.Sprintf("chunk.%s.%d", hash, len(keys)))
		}
		chunks = len(keys)
		root, err := encode(map[string]any{"owner": "capacity-owner", "lease_until": 123456789, "chunks": keys, "hash": hash, "size": len(checkpoint)})
		if err != nil {
			return err
		}
		rootBytes = len(root)
	}
	fmt.Printf("CAPACITY stories=%d links=%d wire=%d raw_proto_bytes=%d snapshot_stored_bytes=%d snapshot_headroom_bytes=%d revisions=%d checkpoint_content_bytes=%d root_bytes=%d chunks=%d max_chunk_bytes=%d\n", len(s.Stories), len(s.Links), len(s.RecentWire), len(raw), len(stored), b.NC().MaxPayload()-int64(len(stored)), len(revisions), len(checkpoint), rootBytes, chunks, 256*1024)
	return nil
}
