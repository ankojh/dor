package agents

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func publishRawArticle(t *testing.T, ctx context.Context, b *bus.Bus, msgID string, raw *dorv1.RawArticle) uint64 {
	t.Helper()
	data, err := proto.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := b.Publish(ctx, bus.SubjectArticleRaw, data, bus.WithMsgID(msgID))
	if err != nil {
		t.Fatalf("publish raw %s: %v", msgID, err)
	}
	return ack.Sequence
}

func waitNormalizerAcked(t *testing.T, ctx context.Context, b *bus.Bus, seq uint64) {
	t.Helper()
	waitForTimeout(t, 30*time.Second, "normalizer consumer ack", func() bool {
		consumer, err := b.JS().Consumer(ctx, bus.StreamForSubject(bus.SubjectArticleRaw), NormalizerDurable)
		if err != nil {
			return false
		}
		info, err := consumer.Info(ctx)
		return err == nil && info.AckFloor.Stream >= seq
	})
}

func subjectMessageCount(t *testing.T, ctx context.Context, b *bus.Bus, subject string) uint64 {
	t.Helper()
	streamName := bus.StreamForSubject(subject)
	stream, err := b.StreamNamed(ctx, streamName)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Subjects[subject]
}

// TestNormalizerConcurrentClaimsPublishOneArticle races several raw items for
// the same canonical URL through the real normalizer. The atomic canonical claim
// (and JetStream dedupe) must yield exactly one clean article.
func TestNormalizerConcurrentClaimsPublishOneArticle(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	metrics := NewMetrics()
	agent, err := NewNormalizer(b, metrics, testLogger(), NormalizerConfig{})
	if err != nil {
		t.Fatalf("new normalizer: %v", err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	now := time.Now().Unix()
	const n = 8
	lastSeq := uint64(0)
	for i := 0; i < n; i++ {
		raw := &dorv1.RawArticle{
			Id:          fmt.Sprintf("raw-concurrent-%d", i),
			SourceId:    fmt.Sprintf("wire-%d", i),
			Url:         "https://example.test/norm-concurrent?utm_source=test",
			Title:       "Storm closes airport runways across the region",
			Summary:     "A powerful storm closed the airport runways on Tuesday morning.",
			PublishedAt: now,
			FetchedAt:   now,
			Lang:        "en",
		}
		seq := publishRawArticle(t, ctx, b, fmt.Sprintf("raw-msg-concurrent-%d", i), raw)
		if seq > lastSeq {
			lastSeq = seq
		}
	}
	waitNormalizerAcked(t, ctx, b, lastSeq)

	if got := subjectMessageCount(t, ctx, b, bus.SubjectArticleClean); got != 1 {
		t.Fatalf("article.clean stored %d messages for one canonical URL, want 1", got)
	}
	published := -1.0
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "dor_normalizer_published_total" && len(family.GetMetric()) == 1 {
			published = family.GetMetric()[0].GetCounter().GetValue()
		}
	}
	if published != 1 {
		t.Fatalf("published metric = %v, want 1", published)
	}
}

// TestNormalizerCanonicalClaimRules exercises the claim itself: redelivery of
// the same raw message id is allowed, while a different raw id for the same
// canonical article is dropped.
func TestNormalizerCanonicalClaimRules(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	metrics := NewMetrics()
	svc := &normalizer{
		bus:     b,
		log:     testLogger(),
		cfg:     NormalizerConfig{Now: time.Now},
		metrics: newNormalizerMetrics(metrics.Registry()),
	}
	raw := &dorv1.RawArticle{
		Id:          "claim-raw",
		SourceId:    "wire",
		Url:         "https://example.test/norm-claim",
		Title:       "Council approves waterfront redevelopment plan",
		Summary:     "The city council approved a waterfront redevelopment plan.",
		PublishedAt: time.Now().Unix(),
		FetchedAt:   time.Now().Unix(),
		Lang:        "en",
	}
	clean, _, reason, ok := svc.normalize(raw)
	if !ok {
		t.Fatalf("normalize failed: %s", reason)
	}

	if got := svc.claim(ctx, "raw-msg-claim", clean); got != "" {
		t.Fatalf("first claim = %q, want accepted", got)
	}
	if got := svc.claim(ctx, "raw-msg-claim", clean); got != "" {
		t.Fatalf("redelivery of the same raw id = %q, want accepted", got)
	}
	if got := svc.claim(ctx, "raw-msg-other", clean); got != "exact" {
		t.Fatalf("different raw id claim = %q, want exact drop", got)
	}
}

// TestNormalizerHazardRevisionsBothPublish checks the hazard revision identity:
// two revisions of one upstream event get distinct article ids, bypass the
// near-duplicate rule and both publish as idempotent updates.
func TestNormalizerHazardRevisionsBothPublish(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	agent, err := NewNormalizer(b, NewMetrics(), testLogger(), NormalizerConfig{})
	if err != nil {
		t.Fatalf("new normalizer: %v", err)
	}
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	now := time.Now().Unix()
	lastSeq := uint64(0)
	for i, revision := range []string{"1", "2"} {
		raw := &dorv1.RawArticle{
			Id:          fmt.Sprintf("hazard-raw-%d", i),
			SourceId:    "usgs",
			Url:         "https://example.test/norm-hazard",
			Title:       "Magnitude 6.1 earthquake strikes the coastal region",
			Summary:     "A magnitude 6.1 earthquake struck the coastal region on Tuesday.",
			PublishedAt: now,
			FetchedAt:   now,
			Lang:        "en",
			Hints: &dorv1.RawHints{
				Hazard:   "earthquake",
				EventId:  "eq-norm-1",
				Revision: revision,
			},
		}
		seq := publishRawArticle(t, ctx, b, fmt.Sprintf("hazard-msg-%d", i), raw)
		if seq > lastSeq {
			lastSeq = seq
		}
	}
	waitNormalizerAcked(t, ctx, b, lastSeq)

	if got := subjectMessageCount(t, ctx, b, bus.SubjectArticleClean); got != 2 {
		t.Fatalf("article.clean stored %d messages, want 2 hazard revisions", got)
	}
	bodies := collectSubject(t, b, bus.SubjectArticleClean)
	if len(bodies) != 2 {
		t.Fatalf("collected %d clean articles, want 2", len(bodies))
	}
	seen := make(map[string]bool, 2)
	for _, body := range bodies {
		var article dorv1.Article
		if err := proto.Unmarshal([]byte(body), &article); err != nil {
			t.Fatalf("decode clean article: %v", err)
		}
		if seen[article.Id] {
			t.Fatalf("hazard revisions share article id %s", article.Id)
		}
		seen[article.Id] = true
		if article.Hints == nil || article.Hints.GetEventId() != "eq-norm-1" {
			t.Fatalf("article %s lost its hazard event identity", article.Id)
		}
	}
}
