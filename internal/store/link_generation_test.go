package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestWriterGenerationFencesDelayedOwnerBatches reproduces the async
// cross-owner ordering: an old writer buffers gen1/gen2, a newer writer
// persists gen3 (link) then gen4 (removal), and only then the old writer
// drains. SQL must keep the newest pair authority, including across a fresh
// store handle, and a later gen5 may legitimately resurrect the pair.
func TestWriterGenerationFencesDelayedOwnerBatches(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("link-gen-%d", time.Now().UnixNano())
	a, b := prefix+"-a", prefix+"-b"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		for _, query := range []string{
			`DELETE FROM links WHERE a=$1 AND b=$2`,
			`DELETE FROM link_generations WHERE a=$1 AND b=$2`,
		} {
			if _, err := st.Pool().Exec(cleanupCtx, query, a, b); err != nil {
				t.Errorf("cleanup %q: %v", query, err)
			}
		}
	})

	oldOpts := DefaultWriterOptions()
	oldOpts.BatchSize = 100
	oldOpts.FlushInterval = time.Hour
	oldOpts.MaxRetries = 0
	old, err := NewWriter(st, oldOpts)
	if err != nil {
		t.Fatal(err)
	}
	old.Start()
	if !old.EnqueueLink(LinkRecord{A: a, B: b, Strength: .1, Confidence: .1, UpdatedAt: time.Now().UTC(), Generation: 1}) ||
		!old.EnqueueLink(LinkRecord{A: a, B: b, Remove: true, Generation: 2}) {
		t.Fatal("old writer did not accept buffered records")
	}

	newOpts := DefaultWriterOptions()
	newOpts.BatchSize = 1
	newOpts.FlushInterval = 10 * time.Millisecond
	newOpts.MaxRetries = 1
	newer, err := NewWriter(st, newOpts)
	if err != nil {
		t.Fatal(err)
	}
	newer.Start()
	t.Cleanup(func() { _ = newer.Close(context.Background()) })
	if !newer.EnqueueLink(LinkRecord{A: a, B: b, Strength: .8, Confidence: .9, UpdatedAt: time.Now().UTC(), Generation: 3}) {
		t.Fatal("newer writer did not accept gen3")
	}
	waitLinkProjection(t, ctx, st, a, b, 3, .8, true)

	if err := old.Close(ctx); err != nil {
		t.Fatalf("old writer drain: %v", err)
	}
	waitLinkProjection(t, ctx, st, a, b, 3, .8, true)

	// Newer removal must win over the older buffered link/removal batch.
	if !newer.EnqueueLink(LinkRecord{A: a, B: b, Remove: true, Generation: 4}) {
		t.Fatal("newer writer did not accept gen4 removal")
	}
	waitLinkProjection(t, ctx, st, a, b, 4, 0, false)

	old2Opts := oldOpts
	old2, err := NewWriter(st, old2Opts)
	if err != nil {
		t.Fatal(err)
	}
	old2.Start()
	if !old2.EnqueueLink(LinkRecord{A: a, B: b, Strength: .2, Confidence: .2, UpdatedAt: time.Now().UTC(), Generation: 1}) ||
		!old2.EnqueueLink(LinkRecord{A: a, B: b, Remove: true, Generation: 2}) {
		t.Fatal("second old writer did not accept buffered records")
	}
	if err := old2.Close(ctx); err != nil {
		t.Fatalf("second old writer drain: %v", err)
	}
	waitLinkProjection(t, ctx, st, a, b, 4, 0, false)

	// A fresh Store sees the durable removal tombstone and no resurrected row.
	restarted, err := Open(ctx, Config{URL: os.Getenv(testDatabaseEnv), ConnectTimeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("open restarted store: %v", err)
	}
	defer restarted.Close()
	waitLinkProjection(t, ctx, restarted, a, b, 4, 0, false)

	// A genuinely newer generation is allowed to reintroduce the link.
	if !newer.EnqueueLink(LinkRecord{A: a, B: b, Strength: .9, Confidence: .95, UpdatedAt: time.Now().UTC(), Generation: 5}) {
		t.Fatal("newer writer did not accept gen5")
	}
	waitLinkProjection(t, ctx, restarted, a, b, 5, .9, true)
}

func waitLinkProjection(t *testing.T, ctx context.Context, st *Store, a, b string, wantGeneration uint64, wantStrength float32, wantPresent bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var generation uint64
		var removed bool
		err := st.Pool().QueryRow(ctx,
			`SELECT generation, removed FROM link_generations WHERE a=$1 AND b=$2`, a, b).
			Scan(&generation, &removed)
		if err == nil && generation >= wantGeneration && removed == !wantPresent {
			if !wantPresent {
				var count int
				if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM links WHERE a=$1 AND b=$2`, a, b).Scan(&count); err == nil && count == 0 {
					return
				}
			} else {
				var strength float32
				if err := st.Pool().QueryRow(ctx, `SELECT strength FROM links WHERE a=$1 AND b=$2`, a, b).Scan(&strength); err == nil && strength == wantStrength {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("link projection stalled: want generation>=%d strength=%v present=%v; last generation=%d removed=%v err=%v",
				wantGeneration, wantStrength, wantPresent, generation, removed, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("link projection context: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
