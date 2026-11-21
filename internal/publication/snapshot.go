package publication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

var cacheMagic = []byte("DOR-SNAPSHOT-2\n")

type cacheManifest struct {
	Blob Blob
	Seq  uint64
}

// PublicProjection preserves the complete live set and protocol representation.
func PublicProjection(snapshot *dorv1.Snapshot) *dorv1.Snapshot {
	r := proto.Clone(snapshot).(*dorv1.Snapshot)
	r.BuiltAt = time.Now().Unix()
	links := r.Links[:0]
	for _, l := range r.Links {
		if l.Strength >= .3 {
			links = append(links, l)
		}
	}
	r.Links = links
	q := func(v float64) float64 { return math.Round(v*1e5) / 1e5 }
	for _, story := range r.Stories {
		for _, loc := range story.Locations {
			if loc != nil {
				loc.Lat = float32(q(float64(loc.Lat)))
				loc.Lon = float32(q(float64(loc.Lon)))
			}
		}
	}
	for _, row := range r.RecentWire {
		row.Lat = q(row.Lat)
		row.Lon = q(row.Lon)
	}
	return r
}

// CacheValue returns ordinary legacy-compatible bytes when small, or a bounded
// manifest to owner-tracked chunks. Failure cannot invalidate committed state.
func (s *Store) CacheValue(ctx context.Context, snapshot *dorv1.Snapshot) ([]byte, error) {
	root, _, err := s.ReadRoot(ctx)
	if err != nil {
		return nil, err
	}
	if root.Owner != s.Owner {
		return nil, ErrFenced
	}
	if snapshot.Seq > root.Seq {
		return nil, errors.New("publication: cache ahead of committed authority")
	}
	started := time.Now()
	data, err := live.MarshalSnapshot(PublicProjection(snapshot))
	s.observe("cache_encode", started, len(data))
	if err != nil {
		return nil, err
	}
	if int64(len(data))+1024 <= s.Limit {
		return data, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.stage(ctx, data)
	if err != nil {
		return nil, err
	}
	_, err = s.update(ctx, func(r *Root) error {
		if snapshot.Seq > r.Seq {
			return errors.New("publication: cache ahead of commit")
		}
		if r.Cache != nil {
			r.Retired = append(r.Retired, Retired{Blob: r.Cache.Blob, At: s.Now().UnixNano()})
		}
		r.Cache = &Entry{Blob: b, Seq: snapshot.Seq}
		removeStage(r, b)
		return nil
	})
	if err != nil {
		return nil, err
	}
	manifest, _ := json.Marshal(cacheManifest{Blob: b, Seq: snapshot.Seq})
	return append(append([]byte(nil), cacheMagic...), manifest...), nil
}
func (s *Store) DecodeCache(ctx context.Context, data []byte) (*dorv1.Snapshot, error) {
	var snap dorv1.Snapshot
	if bytes.HasPrefix(data, cacheMagic) {
		var m cacheManifest
		if err := json.Unmarshal(data[len(cacheMagic):], &m); err != nil {
			return nil, err
		}
		body, err := s.ReadBlob(ctx, m.Blob)
		if err != nil {
			return nil, err
		}
		if err = live.UnmarshalSnapshot(body, &snap); err != nil {
			return nil, err
		}
		if snap.Seq != m.Seq {
			return nil, errors.New("publication: cache watermark")
		}
	} else if err := live.UnmarshalSnapshot(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// Snapshot reads committed authority when available. Cache absence, corruption
// or lag cannot erase it; Pending is deliberately excluded from reconstruction.
func Snapshot(ctx context.Context, publisher, cache jetstream.KeyValue) (*dorv1.Snapshot, error) {
	s := New(publisher, "", 0, 0)
	root, _, err := s.ReadRoot(ctx)
	if err == nil {
		if entry, e := cache.Get(ctx, "snapshot"); e == nil {
			if snap, e := s.DecodeCache(ctx, entry.Value()); e == nil && snap.Seq == root.Seq {
				return snap, nil
			}
		}
		state, _, err := s.Load(ctx)
		if err != nil {
			return nil, err
		}
		return PublicProjection(state.Snapshot()), nil
	}
	if _, e := publisher.Get(ctx, "layout"); e == nil {
		return nil, err
	} else if !errors.Is(e, jetstream.ErrKeyNotFound) {
		return nil, e
	}
	if entry, e := publisher.Get(ctx, RootKey); e == nil {
		var legacy struct {
			Snapshot []byte   `json:"snapshot"`
			Chunks   []string `json:"chunks"`
		}
		body := entry.Value()
		var decodeErr error
		if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
			decodeErr = Decode(body, &legacy)
		} else {
			decodeErr = json.Unmarshal(body, &legacy)
		}
		if decodeErr != nil || len(legacy.Snapshot) == 0 && len(legacy.Chunks) == 0 {
			return nil, err
		}
	} else if !errors.Is(e, jetstream.ErrKeyNotFound) {
		return nil, e
	}
	// Pre-v2 installations may have only the legacy cache. Do not turn missing
	// authority into an empty snapshot, even for a fresh deployment.
	entry, e := cache.Get(ctx, "snapshot")
	if e != nil {
		return nil, e
	}
	return s.DecodeCache(ctx, entry.Value())
}
