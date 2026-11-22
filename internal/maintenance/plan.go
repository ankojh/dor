package maintenance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
)

// AuditArticle is the DB/raw audit input for one article. It deliberately does
// not carry a repair clock: the only accepted original time is an independently
// identified raw fetch time.
type AuditArticle struct {
	ID         string  `json:"id"`
	Published  int64   `json:"published"`
	Created    int64   `json:"created"`
	StoryID    string  `json:"story_id,omitempty"`
	FetchTimes []int64 `json:"raw_fetch_times,omitempty"`
}

// BuildPlan binds the audit to the actual active publisher root and live-state
// revisions and computes a deterministic run identity. It is read-only; the
// caller must already hold the fleet fence and publisher maintenance lease.
func BuildPlan(ctx context.Context, b *bus.Bus, database string, asOf time.Time, audit []AuditArticle, confirmOriginals bool) (*Plan, error) {
	if b == nil {
		return nil, errors.New("maintenance: bus is required")
	}
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return nil, err
	}
	store := publication.New(kv, "", 0, b.NC().MaxPayload())
	root, revision, err := store.ReadRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("maintenance: publisher root unavailable: %w", err)
	}
	state, pending, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if pending != nil || root.Pending != nil {
		return nil, errors.New("maintenance: publisher outbox must be settled unchanged before planning")
	}
	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return nil, err
	}
	var snapshotRevision uint64
	if entry, err := liveKV.Get(ctx, bus.KVLiveStateSnapshotKey); err == nil {
		snapshotRevision = entry.Revision()
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	ingestFirst, ingestLast, err := streamBounds(ctx, b, bus.StreamIngest)
	if err != nil {
		return nil, err
	}
	_, deltaTail, err := streamBounds(ctx, b, bus.StreamDelta)
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Version:          ControlVersion,
		CreatedAt:        time.Now().UnixNano(),
		Database:         database,
		AuditAsOf:        asOf.UnixNano(),
		ConfirmOriginals: confirmOriginals,
		RootRevision:     revision,
		RootOrdinal:      root.Head,
		RootSeq:          root.Seq,
		RootRaw:          root.Raw,
		SnapshotRevision: snapshotRevision,
		IngestFirst:      ingestFirst,
		IngestLast:       ingestLast,
		DeltaTail:        deltaTail,
	}
	auditCopy := normalizedAudit(audit)
	auditJSON, err := CanonicalJSON(auditCopy)
	if err != nil {
		return nil, err
	}
	plan.AuditHash = hashBytes(auditJSON)
	plan.RequestID = RequestIdentity(database, asOf, auditCopy, confirmOriginals)

	policy, err := LoadPolicy(ctx, kv)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		policy = &WireTimePolicy{Version: ControlVersion, Entries: map[string]WireTimeEntry{}}
	}
	plan.BasePolicyHash = policy.Hash()
	entries := make(map[string]WireTimeEntry, len(policy.Entries)+len(auditCopy))
	for id, entry := range policy.Entries {
		entries[id] = entry
	}
	auditByID := make(map[string]AuditArticle, len(auditCopy))
	seen := map[string]bool{}
	for _, a := range auditCopy {
		if a.ID == "" {
			return nil, errors.New("maintenance: audit article has no id")
		}
		if seen[a.ID] {
			return nil, fmt.Errorf("maintenance: duplicate audit article %s", a.ID)
		}
		seen[a.ID] = true
		auditByID[a.ID] = a
		fetch := uniqueFetchTimes(a.FetchTimes)
		entry := WireTimeEntry{
			ArticleID:           a.ID,
			ObservedPublishedAt: a.Published,
			CreatedAt:           a.Created,
			AuditedAt:           asOf.UnixNano(),
			RawEvidenceHash:     hashFetchTimes(fetch),
		}
		// created_at == published_at is copied publication metadata, not an
		// independent clock. Only exactly one positively identified original
		// fetch time, explicitly confirmed by the operator, may override.
		if confirmOriginals && len(fetch) == 1 && fetch[0] > 0 && fetch[0] < a.Published {
			entry.Mode = ModeOverride
			entry.OverrideAt = fetch[0]
			entry.Reason = "confirmed original raw fetch time"
		} else {
			entry.Mode = ModeSuppress
			entry.Reason = "unknown or conflicting original fetch evidence; suppress retained presentation"
			if len(fetch) > 1 {
				entry.Reason = "conflicting raw fetch times; suppress retained presentation"
			}
		}
		entries[a.ID] = entry
	}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		entry := entries[id]
		row := state.Rows[id]
		item := PlanArticle{
			ArticleID:           id,
			ObservedPublishedAt: entry.ObservedPublishedAt,
			CreatedAt:           entry.CreatedAt,
			Mode:                entry.Mode,
			OverrideAt:          entry.OverrideAt,
			Reason:              entry.Reason,
			RawEvidenceHash:     entry.RawEvidenceHash,
			BeforeHash:          rowHash(row),
			Present:             row != nil,
		}
		if a, ok := auditByID[id]; ok {
			item.StoryID = a.StoryID
		} else {
			item.Carried = true
		}
		plan.Articles = append(plan.Articles, item)
		binding := HotArticleBinding{ArticleID: id, Key: live.ArticleKey(id), Mode: entry.Mode, TargetAt: publication.WireSuppressedAt}
		if entry.Mode == ModeOverride {
			binding.TargetAt = entry.OverrideAt
		}
		keyEntry, kerr := liveKV.Get(ctx, binding.Key)
		if kerr == nil {
			binding.Present = true
			binding.Revision = keyEntry.Revision()
			binding.BeforeHash = "sha256:" + hashBytes(keyEntry.Value())
		} else if errors.Is(kerr, jetstream.ErrKeyNotFound) {
			binding.BeforeHash = "absent"
		} else {
			return nil, kerr
		}
		plan.HotArticles = append(plan.HotArticles, binding)
	}
	plan.PolicyHash = plan.Policy().Hash()
	plan.RunID = plan.Hash()
	if err := plan.Verify(); err != nil {
		return nil, err
	}
	return plan, nil
}

// RequestIdentity is independent of evolving retained policy and root state. It
// lets a completed run answer only the same requested audit, never a different
// requested repair.
func RequestIdentity(database string, asOf time.Time, audit []AuditArticle, confirm bool) string {
	payload := struct {
		Database string         `json:"database"`
		AsOf     int64          `json:"as_of"`
		Confirm  bool           `json:"confirm_originals"`
		Audit    []AuditArticle `json:"audit"`
	}{Database: database, AsOf: asOf.UnixNano(), Confirm: confirm, Audit: normalizedAudit(audit)}
	data, _ := CanonicalJSON(payload)
	return hashBytes(data)
}

func normalizedAudit(audit []AuditArticle) []AuditArticle {
	out := append([]AuditArticle(nil), audit...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	for i := range out {
		out[i].FetchTimes = uniqueFetchTimes(out[i].FetchTimes)
	}
	return out
}

// BindPlanToCurrentRoot verifies that the plan still matches the live root and
// snapshot revision. Applying a stale plan fails closed.
func BindPlanToCurrentRoot(ctx context.Context, b *bus.Bus, plan *Plan) error {
	if err := plan.Verify(); err != nil {
		return err
	}
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return err
	}
	store := publication.New(kv, "", 0, b.NC().MaxPayload())
	root, revision, err := store.ReadRoot(ctx)
	if err != nil {
		return err
	}
	if revision != plan.RootRevision || root.Head != plan.RootOrdinal || root.Seq != plan.RootSeq || root.Raw != plan.RootRaw {
		return errors.New("maintenance: publisher root changed since planning")
	}
	if root.Pending != nil {
		return errors.New("maintenance: publisher outbox appeared since planning")
	}
	liveKV, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return err
	}
	var snapshotRevision uint64
	if entry, err := liveKV.Get(ctx, bus.KVLiveStateSnapshotKey); err == nil {
		snapshotRevision = entry.Revision()
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if snapshotRevision != plan.SnapshotRevision {
		return errors.New("maintenance: live snapshot changed since planning")
	}
	state, pending, err := store.Load(ctx)
	if err != nil {
		return err
	}
	if pending != nil {
		return errors.New("maintenance: pending outbox appeared since planning")
	}
	for _, a := range plan.Articles {
		if rowHash(state.Rows[a.ArticleID]) != a.BeforeHash {
			return fmt.Errorf("maintenance: article %s changed since planning", a.ArticleID)
		}
	}
	return nil
}

func streamBounds(ctx context.Context, b *bus.Bus, name string) (uint64, uint64, error) {
	stream, err := b.StreamNamed(ctx, name)
	if err != nil {
		return 0, 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, 0, err
	}
	return info.State.FirstSeq, info.State.LastSeq, nil
}

func uniqueFetchTimes(in []int64) []int64 {
	seen := map[int64]bool{}
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if v <= 0 || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func hashFetchTimes(times []int64) string {
	if len(times) == 0 {
		return ""
	}
	data, _ := CanonicalJSON(times)
	return hashBytes(data)
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// rowHash binds the complete current wire row. Deterministic protobuf
// marshalling sorts map entries, so prompt scores participate in the hash.
func rowHash(row *dorv1.WireRow) string {
	if row == nil {
		return "absent"
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(row)
	if err != nil {
		return "unhashable"
	}
	return "sha256:" + hashBytes(data)
}
