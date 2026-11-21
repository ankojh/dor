package alert

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
)

func deliveryTestBus(t *testing.T) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{ListenAddr: "127.0.0.1:0", StoreDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("connect test bus: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestHitStateAndReceiptsSurviveStoreReopenAndAreGenerationSpecific(t *testing.T) {
	b := deliveryTestBus(t)
	ctx := context.Background()
	first, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h0 := Hit{AlertID: "a", StoryID: "s", Level: "new", Generation: 0, BaseSources: 2, SourceCount: 2, FiredAt: time.Now()}
	h1 := Hit{AlertID: "a", StoryID: "s", Level: "escalated", Generation: 1, BaseSources: 2, SourceCount: 5, FiredAt: time.Now()}
	if h0.Identity() == h1.Identity() {
		t.Fatal("hit identity must include escalation generation")
	}
	if err := first.RecordHit(ctx, h0); err != nil {
		t.Fatal(err)
	}
	if err := first.RecordHit(ctx, h1); err != nil {
		t.Fatal(err)
	}
	if err := first.MarkPushDelivered(ctx, h0, "sub"); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reopened.HitState(ctx, "a", "s")
	if err != nil || !ok {
		t.Fatalf("reopened HitState = %+v, %v, %v", got, ok, err)
	}
	if got.Generation != 1 || got.BaseSources != 2 {
		t.Fatalf("reopened hit state = %+v, want latest generation 1 with base 2", got)
	}
	if delivered, err := reopened.PushDelivered(ctx, h0, "sub"); err != nil || !delivered {
		t.Fatalf("generation 0 receipt = %v, %v; want true", delivered, err)
	}
	if delivered, err := reopened.PushDelivered(ctx, h1, "sub"); err != nil || delivered {
		t.Fatalf("generation 1 receipt = %v, %v; want false independently", delivered, err)
	}
}

func TestReserveRateIsAtomicAcrossStoreInstances(t *testing.T) {
	b := deliveryTestBus(t)
	ctx := context.Background()
	a, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	errs := []error{}
	for i, st := range []*Store{a, second} {
		wg.Add(1)
		go func(i int, st *Store) {
			defer wg.Done()
			ok, _, err := st.ReserveRate(ctx, "alert", string(rune('a'+i)), now, time.Minute)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				allowed++
			}
			if err != nil {
				errs = append(errs, err)
			}
		}(i, st)
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("ReserveRate errors: %v", errs)
	}
	if allowed != 1 {
		t.Fatalf("concurrent reservations granted %d slots, want exactly 1", allowed)
	}
}

func TestDigestUsesConfiguredUTCDeadlineAndFreezesBatchForLateArrivals(t *testing.T) {
	b := deliveryTestBus(t)
	ctx := context.Background()
	st, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 21, 15, 30, 0, 0, time.FixedZone("PST", -8*60*60))
	wantDue := time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)
	if got := NextDigest(at, 8); !got.Equal(wantDue) {
		t.Fatalf("NextDigest = %s, want %s", got, wantDue)
	}
	a := Alert{ID: "digest-alert", Channels: []string{"ntfy"}, DailyDigest: true, DigestHourUTC: 16}
	first := Delivery{ID: "first", Hit: Hit{AlertID: a.ID, StoryID: "s1", Title: "First", FiredAt: at.UTC()}, Channels: []string{"ntfy"}, DueAt: wantDue, Digest: true, DigestAt: wantDue}
	batch, err := st.FreezeDigest(ctx, a, wantDue, []Delivery{first})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Members) != 1 || batch.Members[0] != first.ID {
		t.Fatalf("frozen members = %v, want [first]", batch.Members)
	}
	late := Delivery{ID: "late", Hit: Hit{AlertID: a.ID, StoryID: "s2", Title: "Late", FiredAt: at.Add(time.Hour).UTC()}, Channels: []string{"ntfy"}, DueAt: wantDue, Digest: true, DigestAt: wantDue}
	updated, err := st.FreezeDigest(ctx, a, wantDue, []Delivery{first, late})
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Members) != 1 || updated.Members[0] != first.ID || updated.Hit.Title != batch.Hit.Title {
		t.Fatalf("frozen digest changed after late arrival: %+v", updated)
	}
	pending, err := st.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundLate := false
	for _, d := range pending {
		if d.ID == late.ID {
			foundLate = d.DueAt.Equal(wantDue.AddDate(0, 0, 1))
		}
	}
	if !foundLate {
		t.Fatalf("late arrival not rescheduled after frozen batch: %+v", pending)
	}
}

func TestClaimAlertSerializesReplicas(t *testing.T) {
	b := deliveryTestBus(t)
	ctx := context.Background()
	first, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	release, claimed, err := first.ClaimAlert(ctx, "same-alert", now, time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first claim = claimed %v, err %v", claimed, err)
	}
	defer release()
	_, claimed, err = second.ClaimAlert(ctx, "same-alert", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed {
		t.Fatal("second replica claimed alert while first lease is active")
	}
}

func TestCompletedHitCannotBeReroutedWhenDigestPreferenceChanges(t *testing.T) {
	b := deliveryTestBus(t)
	ctx := context.Background()
	st, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := st.Create(ctx, Alert{ID: "route-alert", Prompt: "wildfire", Enabled: true, Channels: []string{"ntfy"}, DailyDigest: true, DigestHourUTC: 16})
	if err != nil {
		t.Fatal(err)
	}
	fire := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	h := Hit{AlertID: a.ID, StoryID: "route-story", Level: "new", FiredAt: fire, Title: "Wildfire", Prompt: a.Prompt, Generation: 0}
	if err := st.Enqueue(ctx, a, h); err != nil {
		t.Fatal(err)
	}
	pending, err := st.Pending(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("initial pending = %v, err %v", pending, err)
	}
	batch, err := st.FreezeDigest(ctx, a, pending[0].DigestAt, pending)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteDelivery(ctx, batch); err != nil {
		t.Fatal(err)
	}

	noDigest := false
	a, err = st.Update(ctx, a.ID, Patch{DailyDigest: &noDigest})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, a, h); err != nil {
		t.Fatalf("re-enqueue after digest toggle: %v", err)
	}
	pending, err = st.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("completed hit was routed immediately after digest toggle: %+v", pending)
	}

	yesDigest := true
	a, err = st.Update(ctx, a.ID, Patch{DailyDigest: &yesDigest})
	if err != nil {
		t.Fatal(err)
	}
	h.FiredAt = fire.Add(24 * time.Hour) // regenerated matcher hit on a later day
	if err := st.Enqueue(ctx, a, h); err != nil {
		t.Fatalf("re-enqueue on later day: %v", err)
	}
	pending, err = st.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("completed hit was routed into a second digest: %+v", pending)
	}

	routeEntry, err := st.delivery.Get(ctx, "route."+h.Identity())
	if err != nil {
		t.Fatal(err)
	}
	var route Delivery
	if err := json.Unmarshal(routeEntry.Value(), &route); err != nil {
		t.Fatal(err)
	}
	if !route.Digest || !route.DigestAt.Equal(batch.DigestAt) {
		t.Fatalf("stable original route = %+v, want first digest day %s", route, batch.DigestAt)
	}
}

func TestLegacyPushReceiptsImportOnlyFirstTwoGenerations(t *testing.T) {
	b := deliveryTestBus(t)
	ctx := context.Background()
	st, err := New(ctx, b, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.putKV(ctx, "pushdelivered.legacy.story.new", map[string]bool{"subA": true}); err != nil {
		t.Fatal(err)
	}
	if err := st.putKV(ctx, "pushdelivered.legacy.story.escalated", map[string]bool{"subA": true}); err != nil {
		t.Fatal(err)
	}
	for _, hit := range []Hit{
		{AlertID: "legacy", StoryID: "story", Level: "new", Generation: 0},
		{AlertID: "legacy", StoryID: "story", Level: "escalated", Generation: 1},
	} {
		got, err := st.PushDelivered(ctx, hit, "subA")
		if err != nil || !got {
			t.Errorf("legacy generation %d receipt = %v, %v; want imported true", hit.Generation, got, err)
		}
	}
	h3 := Hit{AlertID: "legacy", StoryID: "story", Level: "escalated", Generation: 2}
	got, err := st.PushDelivered(ctx, h3, "subA")
	if err != nil || got {
		t.Fatalf("generation 2 inherited legacy receipt = %v, %v; want false", got, err)
	}
}

func TestReconcileLegacyHitPreservesDBBaselineAndNewestLegacyGeneration(t *testing.T) {
	tests := []struct {
		name            string
		db              Hit
		kv              Hit
		wantGeneration  uint64
		wantSourceCount int
		wantBase        int
	}{
		{
			name:           "db escalation at six supplies base two",
			db:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 6, Generation: 1, BaseSources: 2},
			kv:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 6, Generation: 1},
			wantGeneration: 1, wantSourceCount: 6, wantBase: 2,
		},
		{
			name:           "first db escalation plus later kv escalation",
			db:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 6, Generation: 1, BaseSources: 2},
			kv:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 12, Generation: 1},
			wantGeneration: 2, wantSourceCount: 12, wantBase: 2,
		},
		{
			name:           "db initial hit plus later kv escalation",
			db:             Hit{AlertID: "a", StoryID: "s", Level: "new", SourceCount: 2, Generation: 0, BaseSources: 2},
			kv:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 12, Generation: 1},
			wantGeneration: 2, wantSourceCount: 12, wantBase: 2,
		},
		{
			name:           "newer db generation does not regress",
			db:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 12, Generation: 2, BaseSources: 2},
			kv:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 6, Generation: 1},
			wantGeneration: 2, wantSourceCount: 12, wantBase: 2,
		},
		{
			name:           "db source eight normalizes generation without legacy kv",
			db:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 8, Generation: 1, BaseSources: 2},
			wantGeneration: 2, wantSourceCount: 8, wantBase: 2,
		},
		{
			name:           "db source eight normalizes despite older legacy kv",
			db:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 8, Generation: 1, BaseSources: 2},
			kv:             Hit{AlertID: "a", StoryID: "s", Level: "escalated", SourceCount: 6, Generation: 1},
			wantGeneration: 2, wantSourceCount: 8, wantBase: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := reconcileLegacyHit(tc.db, true, tc.kv, true)
			if !ok {
				t.Fatal("reconciled hit missing")
			}
			if got.Generation != tc.wantGeneration || got.SourceCount != tc.wantSourceCount || got.BaseSources != tc.wantBase {
				t.Fatalf("reconciled hit = %+v, want generation=%d source_count=%d base_sources=%d", got, tc.wantGeneration, tc.wantSourceCount, tc.wantBase)
			}
		})
	}
}
