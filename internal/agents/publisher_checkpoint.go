package agents

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	publisherChunkSize       = 256 * 1024
	publisherCheckpointLimit = 64 * 1024 * 1024
	publisherChunkGrace      = time.Minute
)

// stageCheckpoint writes immutable content before the owner-fenced root CAS.
// A small checkpoint stays inline; an overflow root is a complete ordered
// manifest. Owner/lease live only in the root so renewal reuses the same chunks.
func (c *publisherCore) stageCheckpoint(ctx context.Context, kv jetstream.KeyValue, cp publisherCheckpoint) ([]byte, publisherCheckpoint, error) {
	full := cp
	full.Chunks, full.Hash, full.Size = nil, "", 0
	data, err := encodePublisherCheckpoint(full)
	if err != nil {
		return nil, cp, err
	}
	if int64(len(data))+1024 <= c.bus.NC().MaxPayload() {
		return data, full, nil
	}
	full.Owner, full.LeaseUntil = "", 0
	data, err = encodePublisherCheckpoint(full)
	if err != nil {
		return nil, cp, err
	}
	if len(data) > publisherCheckpointLimit {
		return nil, cp, errors.New("publisher: encoded checkpoint exceeds64MiB")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	// Current-root chunks are protected by GC. Re-publishing a retired/orphan
	// generation must refresh each chunk revision before exposing its root.
	reuse := false
	if entry, err := kv.Get(ctx, publisherCheckpointKey); err == nil {
		var current publisherCheckpoint
		if decodePublisherCheckpoint(entry.Value(), &current) == nil {
			reuse = current.Hash == hash
		}
	}
	var keys []string
	for offset := 0; offset < len(data); offset += publisherChunkSize {
		end := min(offset+publisherChunkSize, len(data))
		key := fmt.Sprintf("chunk.%s.%d", hash, len(keys))
		if !reuse {
			if err := c.checkPayload("checkpoint chunk", data[offset:end]); err != nil {
				return nil, cp, err
			}
			if _, err := kv.Put(ctx, key, data[offset:end]); err != nil {
				return nil, cp, err
			}
		}
		keys = append(keys, key)
	}
	cp.Chunks, cp.Hash, cp.Size = keys, hash, len(data)
	root := publisherCheckpoint{Owner: cp.Owner, LeaseUntil: cp.LeaseUntil, Chunks: keys, Hash: hash, Size: len(data)}
	wire, err := encodePublisherCheckpoint(root)
	if err == nil {
		err = c.checkPayload("checkpoint manifest", wire)
	}
	return wire, cp, err
}

// hydrateCheckpoint fails closed on malformed/partial state. In-flight readers
// of retired generations have a grace minute; a failed caller retries restore
// against the current root rather than accepting incomplete state.
func (c *publisherCore) hydrateCheckpoint(ctx context.Context, kv jetstream.KeyValue, cp *publisherCheckpoint) error {
	if len(cp.Chunks) == 0 {
		return nil
	}
	if cp.Size <= 0 || cp.Size > publisherCheckpointLimit || len(cp.Chunks) != (cp.Size+publisherChunkSize-1)/publisherChunkSize || len(cp.Hash) != 64 {
		return errors.New("publisher: invalid checkpoint manifest bounds")
	}
	data := make([]byte, 0, cp.Size)
	for i, key := range cp.Chunks {
		if key != fmt.Sprintf("chunk.%s.%d", cp.Hash, i) {
			return errors.New("publisher: invalid checkpoint chunk order")
		}
		entry, err := kv.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("publisher: checkpoint chunk %d: %w", i, err)
		}
		want := min(publisherChunkSize, cp.Size-len(data))
		if len(entry.Value()) != want {
			return errors.New("publisher: checkpoint chunk length mismatch")
		}
		data = append(data, entry.Value()...)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != cp.Hash {
		return errors.New("publisher: checkpoint hash mismatch")
	}
	var full publisherCheckpoint
	if err := decodePublisherCheckpoint(data, &full); err != nil {
		return err
	}
	full.Owner, full.LeaseUntil, full.Chunks, full.Hash, full.Size = cp.Owner, cp.LeaseUntil, cp.Chunks, cp.Hash, cp.Size
	*cp = full
	return nil
}

// gcChunks runs under flushMu. Retain recent generations and candidates. Before
// each conditional purge revalidate the ownership root revision. A candidate
// reintroduced by another writer is refreshed before root CAS, so LastRevision
// prevents a stale GC pass from deleting it even during a takeover race.
func (c *publisherCore) gcChunks(ctx context.Context, kv jetstream.KeyValue) error {
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	ctx = opCtx
	root, err := kv.Get(ctx, publisherCheckpointKey)
	if err != nil {
		return err
	}
	var current publisherCheckpoint
	if err := decodePublisherCheckpoint(root.Value(), &current); err != nil {
		return err
	}
	if current.Owner != c.owner {
		return errors.New("publisher: chunk GC owner fenced")
	}
	protected := make(map[string]bool, len(current.Chunks))
	for _, key := range current.Chunks {
		protected[key] = true
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, "chunk.") || protected[key] {
			continue
		}
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if c.now().Sub(entry.Created()) < publisherChunkGrace {
			continue
		}
		latest, err := kv.Get(ctx, publisherCheckpointKey)
		if err != nil {
			return err
		}
		if latest.Revision() != root.Revision() {
			return errors.New("publisher: chunk GC root changed")
		}
		if err := kv.Purge(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil && !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
	}
	return kv.PurgeDeletes(ctx, jetstream.DeleteMarkersOlderThan(publisherChunkGrace))
}

// pruneInputRevisions bounds historical admission metadata. Only inactive
// entities already below their durable consumer's real server AckFloor can be
// forgotten: no buffered, unseen, unacked or staged lower sequence is skipped.
func (c *publisherCore) pruneInputRevisions(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	if !c.restored || c.inflight != nil {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	floors := make(map[string]uint64)
	for _, source := range []struct{ stream, consumer string }{{"ENRICH", "publisher-enrich"}, {"STORY", "publisher-story"}, {"ALERT", "publisher-alert"}} {
		stream, err := c.bus.StreamNamed(opCtx, source.stream)
		if err != nil {
			continue
		}
		consumer, err := stream.Consumer(opCtx, source.consumer)
		if err != nil {
			continue
		}
		info, err := consumer.Info(opCtx)
		if err == nil {
			floors[source.stream] = info.AckFloor.Stream
		}
	}
	c.ingestMu.Lock()
	c.mu.Lock()
	cp := c.checkpoint
	cp.Revisions = make(map[string]uint64, len(c.checkpoint.Revisions))
	changed := false
	for key, seq := range c.checkpoint.Revisions {
		stream, entity, _ := strings.Cut(key, "/")
		stage, id, _ := strings.Cut(entity, ":")
		inactive := false
		switch stage {
		case "clean", "placed", "score":
			inactive = c.wireRows[id] == nil
		case "story":
			inactive = c.stories[id] == nil
		case "link":
			inactive = c.links[id] == nil
		case "alert":
			inactive = true
		}
		if inactive && seq <= floors[stream] && c.revisions[key] == seq {
			delete(c.revisions, key)
			delete(c.committedRevisions, key)
			changed = true
		} else {
			cp.Revisions[key] = seq
		}
	}
	c.mu.Unlock()
	c.ingestMu.Unlock()
	if changed {
		return c.saveCheckpoint(ctx, cp)
	}
	return nil
}
