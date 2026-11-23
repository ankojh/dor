package alert

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/store"
)

func pushOwnerTestStore(t *testing.T, db *store.Store) (*bus.Bus, *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	s, err := New(ctx, b, db, nil)
	if err != nil {
		t.Fatalf("open alert store: %v", err)
	}
	return b, s
}

func TestPushOwnerKVIsolationAndLegacyFailClosed(t *testing.T) {
	ctx := context.Background()
	b, s := pushOwnerTestStore(t, nil)
	if err := s.SavePush(ctx, "", "empty-owner", []byte("ciphertext")); !errors.Is(err, ErrPushOwnerRequired) {
		t.Fatalf("SavePush empty owner error = %v, want ErrPushOwnerRequired", err)
	}
	if err := s.SavePush(ctx, "owner-A", "push-A", []byte("A ciphertext")); err != nil {
		t.Fatal(err)
	}
	if err := s.SavePush(ctx, "owner-B", "push-B", []byte("B ciphertext")); err != nil {
		t.Fatal(err)
	}
	original := []byte("same id original A ciphertext")
	if err := s.SavePush(ctx, "owner-A", "shared-id", original); err != nil {
		t.Fatal(err)
	}
	if err := s.SavePush(ctx, "owner-B", "shared-id", []byte("B must not replace A")); !errors.Is(err, ErrPushOwnerMismatch) {
		t.Fatalf("same-id cross-owner KV save = %v; want ErrPushOwnerMismatch", err)
	}
	recordsA, err := s.PushSubscriptions(ctx, "owner-A")
	if err != nil || len(recordsA) != 2 {
		t.Fatalf("A records after same-id foreign save = %#v, %v; want push-A and shared-id", recordsA, err)
	}
	var shared PushRecord
	if !s.getKV(ctx, "pushsub.shared-id", &shared) || shared.Owner != "owner-A" || string(shared.Blob) != string(original) {
		t.Fatalf("shared KV record after foreign save = %#v; want original A record", shared)
	}
	recordsB, err := s.PushSubscriptions(ctx, "owner-B")
	if err != nil || len(recordsB) != 1 || recordsB[0].ID != "push-B" {
		t.Fatalf("B records after same-id foreign save = %#v, %v; want only push-B", recordsB, err)
	}
	updated := []byte("same id updated A ciphertext")
	if err := s.SavePush(ctx, "owner-A", "shared-id", updated); err != nil {
		t.Fatalf("same-owner save update: %v", err)
	}
	if !s.getKV(ctx, "pushsub.shared-id", &shared) || shared.Owner != "owner-A" || string(shared.Blob) != string(updated) {
		t.Fatalf("shared KV record after same-owner update = %#v; want updated A record", shared)
	}
	if err := s.DeletePush(ctx, "owner-A", "shared-id"); err != nil {
		t.Fatalf("delete shared KV record as owner A: %v", err)
	}
	if err := s.putKV(ctx, "pushsub.legacy", PushRecord{ID: "legacy", Blob: []byte("legacy ciphertext")}); err != nil {
		t.Fatal(err)
	}
	for owner, wantID := range map[string]string{"owner-A": "push-A", "owner-B": "push-B"} {
		records, err := s.PushSubscriptions(ctx, owner)
		if err != nil || len(records) != 1 || records[0].ID != wantID || records[0].Owner != owner {
			t.Fatalf("PushSubscriptions(%q) = %#v, %v; want only %s owned by %s", owner, records, err, wantID, owner)
		}
	}
	if records, err := s.PushSubscriptions(ctx, ""); err != nil || len(records) != 0 {
		t.Fatalf("PushSubscriptions(empty) = %#v, %v; want no records", records, err)
	}
	if err := s.DeletePush(ctx, "owner-A", "legacy"); !errors.Is(err, ErrPushOwnerMismatch) {
		t.Fatalf("DeletePush(owner-A, legacy) = %v, want ErrPushOwnerMismatch", err)
	}
	var legacy PushRecord
	if !s.getKV(ctx, "pushsub.legacy", &legacy) || legacy.Owner != "" {
		t.Fatalf("legacy record after foreign delete = %#v; want unchanged unowned record", legacy)
	}
	if err := s.DeletePush(ctx, "owner-B", "push-A"); !errors.Is(err, ErrPushOwnerMismatch) {
		t.Fatalf("DeletePush(owner-B, push-A) = %v, want ErrPushOwnerMismatch", err)
	}
	var aRecord PushRecord
	if !s.getKV(ctx, "pushsub.push-A", &aRecord) || aRecord.Owner != "owner-A" {
		t.Fatalf("A record after foreign delete = %#v; want it intact", aRecord)
	}
	if err := s.DeletePush(ctx, "owner-A", "push-A"); err != nil {
		t.Fatalf("owner delete: %v", err)
	}
	if s.getKV(ctx, "pushsub.push-A", &aRecord) {
		t.Fatal("owner's delete left push-A in KV")
	}
	_ = b
}

func TestPushOwnerDBMirrorReloadAndDelete(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("DOR_TEST_DATABASE_URL"))
	if url == "" {
		t.Skip("DOR_TEST_DATABASE_URL is not set; skipping push owner DB integration test")
	}
	if strings.Contains(url, "/dor?") || strings.HasSuffix(url, "/dor") {
		t.Fatal("refusing live dor database for push owner integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := store.Open(ctx, store.Config{URL: url})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	var hasOwner, ownerNotNull bool
	var ownerDefault *string
	err = db.Pool().QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema=current_schema() AND table_name='push_subscriptions' AND column_name='owner'
		), COALESCE((SELECT is_nullable='NO' FROM information_schema.columns
			WHERE table_schema=current_schema() AND table_name='push_subscriptions' AND column_name='owner'), false),
		(SELECT column_default FROM information_schema.columns
			WHERE table_schema=current_schema() AND table_name='push_subscriptions' AND column_name='owner')`).Scan(&hasOwner, &ownerNotNull, &ownerDefault)
	if err != nil || !hasOwner || !ownerNotNull || ownerDefault == nil || !strings.Contains(*ownerDefault, "''") {
		t.Fatalf("push_subscriptions.owner schema = exists:%v not-null:%v default:%v err:%v; want NOT NULL default empty string", hasOwner, ownerNotNull, ownerDefault, err)
	}

	b, s := pushOwnerTestStore(t, db)
	ownerA, ownerB := "ownerA_"+randomHex(8), "ownerB_"+randomHex(8)
	idA, idB := "pushA_"+randomHex(8), "pushB_"+randomHex(8)
	idShared := "pushShared_" + randomHex(8)
	t.Cleanup(func() {
		_, _ = db.Pool().Exec(context.Background(), `DELETE FROM push_subscriptions WHERE id = ANY($1)`, []string{idA, idB, idShared})
		kv, err := b.KV(context.Background(), bus.KVBucketLiveState)
		if err == nil {
			_ = kv.Purge(context.Background(), "pushsub."+idA)
			_ = kv.Purge(context.Background(), "pushsub."+idB)
			_ = kv.Purge(context.Background(), "pushsub."+idShared)
		}
	})
	if err := s.SavePush(ctx, ownerA, idA, []byte("ciphertext A")); err != nil {
		t.Fatal(err)
	}
	if err := s.SavePush(ctx, ownerB, idB, []byte("ciphertext B")); err != nil {
		t.Fatal(err)
	}
	var persistedA, persistedB string
	if err := db.Pool().QueryRow(ctx, `SELECT owner FROM push_subscriptions WHERE id=$1`, idA).Scan(&persistedA); err != nil || persistedA != ownerA {
		t.Fatalf("persisted A owner = %q, %v; want %q", persistedA, err, ownerA)
	}
	if err := db.Pool().QueryRow(ctx, `SELECT owner FROM push_subscriptions WHERE id=$1`, idB).Scan(&persistedB); err != nil || persistedB != ownerB {
		t.Fatalf("persisted B owner = %q, %v; want %q", persistedB, err, ownerB)
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Purge(ctx, "pushsub."+idA); err != nil {
		t.Fatal(err)
	}
	if err := kv.Purge(ctx, "pushsub."+idB); err != nil {
		t.Fatal(err)
	}
	fresh, err := New(ctx, b, db, nil)
	if err != nil {
		t.Fatalf("construct fresh alert store: %v", err)
	}
	gotA, err := fresh.PushSubscriptions(ctx, ownerA)
	if err != nil || len(gotA) != 1 || gotA[0].ID != idA || gotA[0].Owner != ownerA {
		t.Fatalf("DB fallback for A = %#v, %v; want only A", gotA, err)
	}
	if err := fresh.DeletePush(ctx, ownerB, idA); !errors.Is(err, ErrPushOwnerMismatch) {
		t.Fatalf("foreign DB-only delete = %v, want ErrPushOwnerMismatch", err)
	}
	var n int
	if err := db.Pool().QueryRow(ctx, `SELECT count(*) FROM push_subscriptions WHERE id=$1`, idA).Scan(&n); err != nil || n != 1 {
		t.Fatalf("DB row after foreign delete = %d, %v; want 1", n, err)
	}
	if err := fresh.DeletePush(ctx, ownerA, idA); err != nil {
		t.Fatalf("owner DB-only delete: %v", err)
	}
	var recs []PushRecord
	for _, owner := range []string{ownerA, ownerB} {
		recs, err = fresh.PushSubscriptions(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if owner == ownerA && len(recs) != 0 {
			t.Fatalf("A records after delete = %#v", recs)
		}
	}
	if err := db.Pool().QueryRow(ctx, `SELECT count(*) FROM push_subscriptions WHERE id=$1`, idA).Scan(&n); err != nil || n != 0 {
		t.Fatalf("DB row after owner delete = %d, %v; want 0", n, err)
	}
	sharedOwnerA, sharedOwnerB := "sharedOwnerA_"+randomHex(8), "sharedOwnerB_"+randomHex(8)
	sharedA := []byte("DB-only original A ciphertext")
	if err := s.SavePush(ctx, sharedOwnerA, idShared, sharedA); err != nil {
		t.Fatalf("save DB-only A record: %v", err)
	}
	if err := kv.Purge(ctx, "pushsub."+idShared); err != nil {
		t.Fatalf("purge shared A KV record: %v", err)
	}
	if err := s.SavePush(ctx, sharedOwnerB, idShared, []byte("DB-only B replacement attempt")); !errors.Is(err, ErrPushOwnerMismatch) {
		t.Fatalf("DB-only same-id cross-owner save = %v; want ErrPushOwnerMismatch", err)
	}
	var rolledBack PushRecord
	if s.getKV(ctx, "pushsub."+idShared, &rolledBack) {
		t.Fatalf("KV row after DB-only mismatch rollback = %#v; want absent", rolledBack)
	}
	var sharedPersistedOwner string
	if err := db.Pool().QueryRow(ctx, `SELECT owner FROM push_subscriptions WHERE id=$1`, idShared).Scan(&sharedPersistedOwner); err != nil || sharedPersistedOwner != sharedOwnerA {
		t.Fatalf("DB owner after same-id mismatch = %q, %v; want %q", sharedPersistedOwner, err, sharedOwnerA)
	}
	sharedFresh, err := New(ctx, b, db, nil)
	if err != nil {
		t.Fatalf("construct fresh store after mismatch rollback: %v", err)
	}
	gotSharedA, err := sharedFresh.PushSubscriptions(ctx, sharedOwnerA)
	if err != nil || len(gotSharedA) != 1 || gotSharedA[0].ID != idShared || gotSharedA[0].Owner != sharedOwnerA || string(gotSharedA[0].Blob) != string(sharedA) {
		t.Fatalf("DB-only shared A records after rollback = %#v, %v; want original A row", gotSharedA, err)
	}
	gotSharedB, err := sharedFresh.PushSubscriptions(ctx, sharedOwnerB)
	if err != nil || len(gotSharedB) != 0 {
		t.Fatalf("DB-only shared B records after rollback = %#v, %v; want none", gotSharedB, err)
	}
}

func TestPushOwnerKVRegistrationSurvivesDBMirrorOutage(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("DOR_TEST_DATABASE_URL"))
	if url == "" {
		t.Skip("DOR_TEST_DATABASE_URL is not set; skipping push owner DB outage test")
	}
	if strings.Contains(url, "/dor?") || strings.HasSuffix(url, "/dor") {
		t.Fatal("refusing live dor database for push owner DB outage test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := store.Open(ctx, store.Config{URL: url})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	db.Close() // Force savePushDB to fail through a closed pool.

	b, withClosedDB := pushOwnerTestStore(t, db)
	owner := "outageOwner_" + randomHex(8)
	id := "outagePush_" + randomHex(8)
	blob := []byte("synthetic encrypted subscription")
	t.Cleanup(func() {
		kv, err := b.KV(context.Background(), bus.KVBucketLiveState)
		if err == nil {
			_ = kv.Purge(context.Background(), "pushsub."+id)
		}
	})
	if err := withClosedDB.SavePush(ctx, owner, id, blob); err != nil {
		t.Fatalf("SavePush with unavailable DB mirror = %v; want successful KV-first registration", err)
	}
	// Read the authoritative KV through a store without the failed DB mirror;
	// PushSubscriptions on the outage-bound store would also query the closed
	// DB and report that separate read error.
	kvOnly, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatalf("construct KV-only read store: %v", err)
	}
	records, err := kvOnly.PushSubscriptions(ctx, owner)
	if err != nil || len(records) != 1 || records[0].ID != id || records[0].Owner != owner || string(records[0].Blob) != string(blob) {
		t.Fatalf("KV registration after mirror outage = %#v, %v; want original owner/id/blob", records, err)
	}

	reopened, err := store.Open(ctx, store.Config{URL: url})
	if err != nil {
		t.Fatalf("reopen test database: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	var rows int
	if err := reopened.Pool().QueryRow(ctx, `SELECT count(*) FROM push_subscriptions WHERE id=$1`, id).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("DB rows for failed mirror ID = %d, %v; want none", rows, err)
	}
}

func TestPushOwnerUnownedLegacyRecordIsNotReturned(t *testing.T) {
	ctx := context.Background()
	_, s := pushOwnerTestStore(t, nil)
	if err := s.putKV(ctx, "pushsub.legacy-only", PushRecord{ID: "legacy-only", Blob: []byte("legacy")}); err != nil {
		t.Fatal(err)
	}
	records, err := s.PushSubscriptions(ctx, "known-owner")
	if err != nil || len(records) != 0 {
		t.Fatalf("legacy returned to owner: %#v, %v", records, err)
	}
	var raw PushRecord
	if !s.getKV(ctx, "pushsub.legacy-only", &raw) || raw.Owner != "" {
		t.Fatalf("legacy record was changed: %#v", raw)
	}
}

func TestPushOwnerRecordJSONCarriesOwner(t *testing.T) {
	rec := PushRecord{ID: "id", Owner: "owner", Blob: []byte{1, 2}}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PushRecord
	if err := json.Unmarshal(b, &decoded); err != nil || decoded.Owner != "owner" {
		t.Fatalf("PushRecord round trip = %#v, %v", decoded, err)
	}
}
