package agents

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
)

// TestClusterJoinTombstoneRetirement covers the bounded-retention fix: completed
// joins leave delete markers, retireTombstones purges markers older than the
// retention window, and a live pending join is never touched. Repeated cycles
// must plateau in physical stream size instead of growing linearly.
func TestClusterJoinTombstoneRetirement(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	oldRetention := clusterTombstoneRetention
	clusterTombstoneRetention = -time.Second // purge every marker regardless of age
	defer func() { clusterTombstoneRetention = oldRetention }()

	c := newGoldenClusterer(t, b)
	joins, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		t.Fatal(err)
	}
	pending := goldenArticle("retained-pending", "wire", "Still waiting for enrichment", "", time.Now().Unix())
	putJoinState(t, ctx, b, pending.Id, joinState{Article: pending})

	stream, err := b.StreamNamed(ctx, "KV_joins")
	if err != nil {
		t.Fatal(err)
	}
	firstBytes := uint64(0)
	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("join.done-%d-%d", cycle, i)
			if _, err := joins.Put(ctx, key, []byte("{}")); err != nil {
				t.Fatal(err)
			}
			if err := joins.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
		c.retireTombstones(ctx)

		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := joins.Get(ctx, "join."+pending.Id); err != nil {
			t.Fatalf("cycle %d purged the live pending join: %v", cycle, err)
		}
		if info.State.Msgs != 1 {
			t.Fatalf("cycle %d retained %d messages after purge, want only the pending join", cycle, info.State.Msgs)
		}
		if cycle == 0 {
			firstBytes = info.State.Bytes
			continue
		}
		if info.State.Bytes > firstBytes+1024 {
			t.Fatalf("physical bytes grew across purge cycles: %d -> %d", firstBytes, info.State.Bytes)
		}
	}
}

// TestRelationUpgradeReplaysNewSubjects verifies the accepted upgrade path: a
// widened relation-v2 durable with DeliverAll replays messages the narrow legacy
// durable already acknowledged, and the legacy durable is retired only after the
// new one catches up.
func TestRelationUpgradeReplaysNewSubjects(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	now := time.Now().Unix()
	owned := relationTestStory("upgrade-owned", now, 0.8, 40, -74)
	other := relationTestStory("upgrade-other", now, 0.8, 40, -74)
	putRelationMeta(t, ctx, b, owned.Id, goldenBase(), []string{"Shared"}, now-600, now)
	putRelationMeta(t, ctx, b, other.Id, goldenBase(), []string{"Shared"}, now-600, now)

	// The legacy consumer only saw every third shard and already acked its
	// subset.
	oldFilters := make([]string, 0, bus.StoryShardCount/3+1)
	for i := 0; i < bus.StoryShardCount; i += 3 {
		oldFilters = append(oldFilters, bus.StoryChangedSubjectForShard(i))
	}
	consumer, err := b.EnsureConsumer(ctx, bus.ConsumerOptions{Durable: "relation-0", FilterSubjects: oldFilters})
	if err != nil {
		t.Fatal(err)
	}
	// Publish both stories on their story.changed shard subjects so the legacy
	// filter excludes one of them.
	otherData, err := proto.Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	ownedData, err := proto.Marshal(owned)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, bus.StoryChangedSubjectForShard(1), otherData, bus.WithMsgID("upgrade-other")); err != nil {
		t.Fatal(err)
	}
	last, err := b.Publish(ctx, bus.StoryChangedSubjectForShard(0), ownedData, bus.WithMsgID("upgrade-owned"))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}

	agent, err := NewRelationJudge(b, NewMetrics(), testLogger(), RelationConfig{
		Shards:   3,
		Shard:    0,
		Debounce: 20 * time.Millisecond,
		Workers:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	core := agent.(*relationAgent).core
	runner := startRunner(t, agent)
	defer stopRunner(t, runner)

	waitForTimeout(t, 30*time.Second, "new durable replays both stories", func() bool {
		core.mu.Lock()
		_, hasOwned := core.index[owned.Id]
		_, hasOther := core.index[other.Id]
		core.mu.Unlock()
		return hasOwned && hasOther
	})
	newConsumer, err := b.JS().Consumer(ctx, bus.StreamForSubject(bus.StoryChangedSubject("x")), "relation-v2-0")
	if err != nil {
		t.Fatal(err)
	}
	waitForTimeout(t, 30*time.Second, "new durable drains", func() bool {
		info, err := newConsumer.Info(ctx)
		return err == nil && info.NumPending == 0 && info.NumAckPending == 0 && info.AckFloor.Stream >= last.Sequence
	})
	waitForTimeout(t, 30*time.Second, "legacy durable retired", func() bool {
		_, err := b.JS().Consumer(ctx, bus.StreamForSubject(bus.StoryChangedSubject("x")), "relation-0")
		return errors.Is(err, jetstream.ErrConsumerNotFound)
	})
}
