package bus_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
)

func testBus(t *testing.T, storeDir string) *bus.Bus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{
		ListenAddr: "127.0.0.1:0",
		StoreDir:   storeDir,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("connect bus: %v", err)
	}
	return b
}

func TestStoryShard(t *testing.T) {
	cases := map[string]int{
		"":                            5,
		"a":                           12,
		"story-1":                     4,
		"story-2":                     13,
		"hello world":                 7,
		"article:https://example.com": 7,
	}
	for id, want := range cases {
		if got := bus.StoryShard(id); got != want {
			t.Errorf("StoryShard(%q) = %d, want %d", id, got, want)
		}
		if got := bus.StoryShard(id); got < 0 || got >= bus.StoryShardCount {
			t.Errorf("StoryShard(%q) = %d outside 0..%d", id, got, bus.StoryShardCount-1)
		}
	}
	if got, want := bus.StoryChangedSubject("story-1"), "story.changed.4"; got != want {
		t.Errorf("StoryChangedSubject = %q, want %q", got, want)
	}
	if got, want := bus.StoryChangedSubjectForShard(15), "story.changed.15"; got != want {
		t.Errorf("StoryChangedSubjectForShard = %q, want %q", got, want)
	}
}

func TestSubjectConstants(t *testing.T) {
	want := map[string]string{
		"article.raw":          bus.SubjectArticleRaw,
		"article.raw.priority": bus.SubjectArticleRawPriority,
		"article.clean":        bus.SubjectArticleClean,
		"article.place":        bus.SubjectArticlePlace,
		"article.class":        bus.SubjectArticleClass,
		"article.vec":          bus.SubjectArticleVec,
		"link.changed":         bus.SubjectLinkChanged,
		"article.placed":       bus.SubjectArticlePlaced,
		"match.alert":          bus.SubjectMatchAlert,
		"delta.out":            bus.SubjectDeltaOut,
		"story.changed.":       bus.SubjectStoryChangedPrefix,
		"match.filter.":        bus.SubjectMatchFilterPrefix,
	}
	for expected, actual := range want {
		if expected != actual {
			t.Errorf("subject constant changed: want %q, got %q", expected, actual)
		}
	}
	if got := bus.MatchFilterSubject("client-1"); got != "match.filter.client-1" {
		t.Errorf("MatchFilterSubject = %q", got)
	}
}

func TestStreamAndBuckets(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	policies := bus.DefaultStreamPolicies()
	for _, name := range bus.StreamOrder() {
		want := policies[name]
		stream, err := b.StreamNamed(ctx, name)
		if err != nil {
			t.Fatalf("lookup stream %s: %v", name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("stream %s info: %v", name, err)
		}
		if info.Config.Storage != want.Storage {
			t.Errorf("%s storage = %v, want %v", name, info.Config.Storage, want.Storage)
		}
		if info.Config.MaxAge != want.MaxAge {
			t.Errorf("%s max age = %s, want %s", name, info.Config.MaxAge, want.MaxAge)
		}
		if info.Config.MaxBytes != want.MaxBytes {
			t.Errorf("%s max bytes = %d, want %d", name, info.Config.MaxBytes, want.MaxBytes)
		}
		if info.Config.Duplicates != want.Duplicates {
			t.Errorf("%s duplicate window = %s, want %s", name, info.Config.Duplicates, want.Duplicates)
		}
		if !sameStringSet(info.Config.Subjects, want.Subjects) {
			t.Errorf("%s subjects = %v, want %v", name, info.Config.Subjects, want.Subjects)
		}
	}

	wantTTL := map[string]time.Duration{
		bus.KVBucketSeen:      bus.KVSeenTTL,
		bus.KVBucketJoins:     bus.KVJoinsTTL,
		bus.KVBucketBudgets:   bus.KVBudgetsTTL,
		bus.KVBucketSpend:     bus.KVSpendTTL,
		bus.KVBucketLiveState: bus.KVLiveStateTTL,
	}
	for bucket, ttl := range wantTTL {
		kv, err := b.KV(ctx, bucket)
		if err != nil {
			t.Fatalf("kv %s: %v", bucket, err)
		}
		status, err := kv.Status(ctx)
		if err != nil {
			t.Fatalf("kv %s status: %v", bucket, err)
		}
		if status.TTL() != ttl {
			t.Errorf("kv %s ttl = %s, want %s", bucket, status.TTL(), ttl)
		}
	}

	if got := bus.StreamForSubject(bus.SubjectArticleRaw); got != bus.StreamIngest {
		t.Errorf("article.raw stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.SubjectArticleClean); got != bus.StreamEnrich {
		t.Errorf("article.clean stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.StoryChangedSubject("x")); got != bus.StreamStory {
		t.Errorf("story.changed stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.SubjectMatchAlert); got != bus.StreamAlert {
		t.Errorf("match.alert stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.SubjectDeltaOut); got != bus.StreamDelta {
		t.Errorf("delta.out stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.MatchFilterSubject("c")); got != bus.StreamMatch {
		t.Errorf("match.filter stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.DLQSubject("a")); got != bus.StreamDLQ {
		t.Errorf("dlq stream = %s", got)
	}
	if got := bus.StreamForSubject(bus.SubjectEchoOut); got != bus.StreamEcho {
		t.Errorf("echo stream = %s", got)
	}
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	return true
}

func TestPublishSetsMsgIDAndDeduplicates(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	before, err := b.Stream().Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	first, err := b.Publish(ctx, bus.SubjectEchoOut, []byte("one"), bus.WithMsgID("fixed-id"))
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first publish reported duplicate")
	}
	second, err := b.Publish(ctx, bus.SubjectEchoOut, []byte("two"), bus.WithMsgID("fixed-id"))
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if !second.Duplicate {
		t.Fatal("second publish was not deduplicated")
	}
	after, err := b.Stream().Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if after.State.Msgs != before.State.Msgs+1 {
		t.Fatalf("stream messages = %d, want %d", after.State.Msgs, before.State.Msgs+1)
	}

	// A publish without an explicit id still carries Nats-Msg-Id.
	auto, err := b.Publish(ctx, bus.SubjectEchoOut, []byte("three"))
	if err != nil {
		t.Fatalf("auto-id publish: %v", err)
	}
	if auto.Duplicate {
		t.Fatal("auto-id publish unexpectedly deduplicated")
	}
}

func TestConsumerDefaultsAndOverride(t *testing.T) {
	b := testBus(t, t.TempDir())
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	defaultConsumer, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       "default-pending",
		FilterSubject: bus.SubjectEchoIn,
	})
	if err != nil {
		t.Fatalf("ensure default consumer: %v", err)
	}
	info, err := defaultConsumer.Info(ctx)
	if err != nil {
		t.Fatalf("default consumer info: %v", err)
	}
	if info.Config.MaxAckPending != b.MaxAckPending() {
		t.Errorf("MaxAckPending = %d, want %d", info.Config.MaxAckPending, b.MaxAckPending())
	}
	if info.Config.AckWait != 30*time.Second {
		t.Errorf("AckWait = %s, want 30s", info.Config.AckWait)
	}

	override, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       "override-pending",
		FilterSubject: bus.SubjectEchoIn,
		MaxAckPending: 7,
		AckWait:       2 * time.Second,
		MaxDeliver:    3,
	})
	if err != nil {
		t.Fatalf("ensure override consumer: %v", err)
	}
	info, err = override.Info(ctx)
	if err != nil {
		t.Fatalf("override consumer info: %v", err)
	}
	if info.Config.MaxAckPending != 7 || info.Config.AckWait != 2*time.Second || info.Config.MaxDeliver != 3 {
		t.Errorf("override not applied: %+v", info.Config)
	}
}

func TestDurableConsumerResumesAcrossBusRestart(t *testing.T) {
	store := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	b := testBus(t, store)
	for i := 0; i < 5; i++ {
		id := "resume-" + string(rune('a'+i))
		if _, err := b.Publish(ctx, bus.SubjectEchoOut, []byte(id), bus.WithMsgID(id)); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}

	consumer, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       "bus-resume-test",
		FilterSubject: bus.SubjectEchoOut,
		MaxAckPending: 8,
		AckWait:       2 * time.Second,
	})
	if err != nil {
		t.Fatalf("ensure consumer: %v", err)
	}
	received := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("next message %d: %v", i, err)
		}
		received = append(received, string(msg.Data()))
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatalf("double ack %d: %v", i, err)
		}
	}
	if received[0] != "resume-a" || received[1] != "resume-b" {
		t.Fatalf("first messages = %v, want [resume-a resume-b]", received)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close first bus: %v", err)
	}

	b2 := testBus(t, store)
	defer b2.Close()
	consumer2, err := b2.EnsureConsumer(ctx, bus.ConsumerOptions{
		Durable:       "bus-resume-test",
		FilterSubject: bus.SubjectEchoOut,
		MaxAckPending: 8,
		AckWait:       2 * time.Second,
	})
	if err != nil {
		t.Fatalf("ensure consumer after restart: %v", err)
	}

	var got []string
	for i := 0; i < 3; i++ {
		msg, err := consumer2.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatalf("next resumed message %d: %v (got %v)", i, err, got)
		}
		got = append(got, string(msg.Data()))
		_ = msg.Ack()
	}
	want := []string{"resume-c", "resume-d", "resume-e"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resumed messages = %v, want %v", got, want)
		}
	}
	if _, err := consumer2.Next(jetstream.FetchMaxWait(500 * time.Millisecond)); err == nil {
		t.Fatal("unexpected extra delivery: already acked messages were replayed")
	}
}
