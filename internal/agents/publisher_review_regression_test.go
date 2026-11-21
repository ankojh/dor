package agents

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
	"google.golang.org/protobuf/proto"
)

func TestPublisherSustainedFlushFailureIsReported(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	metrics := NewMetrics()
	agent, err := NewPublisher(b, metrics, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	p := agent.(*publisherAgent)
	if err := p.core.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer p.core.release(context.Background())
	p.core.publish = func(context.Context, []byte, string) error {
		return errors.New("injected DELTA publish outage")
	}
	p.core.onStory(&dorv1.Story{Id: "flush-error", Title: "must remain pending"})

	loopCtx, stop := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		p.core.flushLoop(loopCtx, 5*time.Millisecond)
	}()
	t.Cleanup(func() {
		stop()
		<-loopDone
	})
	waitFor(t, "publisher flush error reporting", func() bool { return p.LastError() != nil })
	if got := p.LastError(); got == nil || !strings.Contains(got.Error(), "injected DELTA publish outage") {
		t.Fatalf("LastError()=%v, want injected publish failure", got)
	}
	if count := gatheredCounter(t, metrics, "dor_agent_errors_total", map[string]string{"agent": PublisherAgentName}); count == 0 {
		t.Fatal("flush failure did not increment publisher error metric")
	}
	if _, n := metricP95(metrics, "dor_agent_handle_seconds", map[string]string{"agent": PublisherAgentName, "outcome": OutcomeRetry}); n == 0 {
		t.Fatal("flush failure did not record a retry outcome in publisher latency metrics")
	}
	if _, n := metricP95(metrics, "dor_publisher_storage_seconds", map[string]string{"operation": "root_write"}); n == 0 {
		t.Fatal("failed flush did not record its authoritative reservation root write")
	}
	p.core.mu.Lock()
	defer p.core.mu.Unlock()
	if p.core.inflight == nil || !p.core.inflight.reserved || p.core.seq != 0 {
		t.Fatalf("failed flush did not preserve immutable pending batch: inflight=%+v seq=%d", p.core.inflight, p.core.seq)
	}
}

func gatheredCounter(t *testing.T, metrics *Metrics, familyName string, labels map[string]string) float64 {
	t.Helper()
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.GetMetric() {
			matches := true
			for key, value := range labels {
				found := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == key && label.GetValue() == value {
						found = true
					}
				}
				if !found {
					matches = false
					break
				}
			}
			if matches && metric.GetCounter() != nil {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestPublisherFencingCancelsAcceptWaitersAndRunContext(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agent, err := NewPublisher(b, NewMetrics(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	p := agent.(*publisherAgent)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		_ = p.Stop(stopCtx)
	}()

	accepted := make(chan error, 1)
	go func() {
		accepted <- p.core.accept(context.Background(), func() { p.core.onStory(&dorv1.Story{Id: "blocked"}) })
	}()
	waitFor(t, "accepted commit waiter", func() bool {
		p.core.mu.Lock()
		defer p.core.mu.Unlock()
		return len(p.core.waiters) == 1
	})
	// Add a second admission that must wait for capacity. Fencing must wake both
	// kinds of waiter, including the one that has not yet registered a commit.
	p.core.mu.Lock()
	p.core.pendingCredit = int64(b.NC().MaxPayload())
	p.core.mu.Unlock()
	blocked := make(chan error, 1)
	go func() {
		blocked <- p.core.acceptInput(context.Background(), &Message{Subject: bus.SubjectArticleClean, Data: []byte("credit")}, "clean:capacity", func() {})
	}()
	select {
	case <-p.runCtx.Done():
		t.Fatal("publisher run context stopped before fencing")
	default:
	}
	if err := p.core.checkFence(publication.ErrFenced); !errors.Is(err, publication.ErrFenced) {
		t.Fatalf("checkFence()=%v, want ErrFenced", err)
	}
	select {
	case <-p.runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("fencing did not promptly cancel the publisher run context")
	}
	for name, ch := range map[string]<-chan error{"commit waiter": accepted, "capacity waiter": blocked} {
		select {
		case err := <-ch:
			if err == nil {
				t.Errorf("%s returned success after fencing", name)
			}
		case <-time.After(time.Second):
			t.Errorf("%s remained blocked after fencing", name)
		}
	}
}

func TestPublisherOversizedInputRetriesBeforeReservationOrAck(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	metrics := NewMetrics()
	agent, err := NewPublisher(b, metrics, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	p := agent.(*publisherAgent)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		_ = p.Stop(stopCtx)
	}()

	stream, err := b.JS().Stream(ctx, bus.StreamEnrich)
	if err != nil {
		t.Fatal(err)
	}
	const subject = bus.SubjectArticleClean
	// JetStream accepts the message, but the safe input->DELTA budget (message
	// bytes plus admission/header overhead) must reject it before state or
	// immutable journal reservation is changed.
	maxPayload := int(b.NC().MaxPayload())
	article := &dorv1.Article{Id: "too-large", Title: strings.Repeat("x", maxPayload-1200)}
	data, err := proto.Marshal(article)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) >= maxPayload {
		t.Fatalf("oversize fixture cannot be published: encoded %d >= max payload %d", len(data), maxPayload)
	}
	if _, err := b.Publish(ctx, subject, data, bus.WithMsgID("oversized-admission")); err != nil {
		t.Fatalf("publish input fixture: %v", err)
	}
	consumer, err := b.JS().Consumer(ctx, bus.StreamEnrich, "publisher-enrich")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "oversized input rejected by publisher admission", func() bool {
		return gatheredCounter(t, metrics, "dor_agent_errors_total", map[string]string{"agent": "publisher-enrich"}) > 0
	})
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	last, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		t.Fatal(err)
	}
	if info.Delivered.Stream < last.Sequence || info.AckFloor.Stream >= last.Sequence {
		t.Fatalf("oversized input was not redelivered/unacked: delivered=%d ackFloor=%d input=%d ackPending=%d pending=%d", info.Delivered.Stream, info.AckFloor.Stream, last.Sequence, info.NumAckPending, info.NumPending)
	}

	root, _, err := p.core.storage.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if root.Pending != nil || len(root.Entries) != 0 || root.Seq != 0 || root.Raw != 0 {
		t.Fatalf("oversized input reached immutable reservation/commit: root=%+v", root)
	}
	p.core.mu.Lock()
	_, mutated := p.core.wireRows[article.Id]
	_, revision := p.core.revisions[bus.StreamEnrich+"/clean:"+article.Id]
	queued := len(p.core.pending) + len(p.core.waiters) + len(p.core.order)
	p.core.mu.Unlock()
	if mutated || revision || queued != 0 {
		t.Fatalf("oversized admission mutated state: wire=%t revision=%t queued=%d", mutated, revision, queued)
	}
	if got := last.Header.Get(bus.HeaderMsgID); got != "oversized-admission" {
		t.Fatalf("source record changed during retries: msg-id=%q", got)
	}
}

func TestPublisherOversizedDeltaBatchStaysUnreservedAndUnacked(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newPublisherCore(b, testLogger())
	if err := c.restore(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.release(context.Background())
	ack := make(chan error, 1)
	story := &dorv1.Story{Id: "oversized-batch", Title: strings.Repeat("z", int(b.NC().MaxPayload()))}
	go func() { ack <- c.accept(ctx, func() { c.onStory(story) }) }()
	waitFor(t, "oversized story batch admission", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.pending) > 0
	})
	if err := c.flush(ctx); err == nil {
		t.Fatal("oversized DELTA batch was reserved instead of rejected")
	}
	select {
	case err := <-ack:
		t.Fatalf("oversized batch input acknowledged before durable reservation: %v", err)
	default:
	}
	root, _, err := c.storage.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if root.Pending != nil || len(root.Entries) != 0 || root.Seq != 0 {
		t.Fatalf("oversized batch changed authoritative journal: pending=%v entries=%d seq=%d", root.Pending, len(root.Entries), root.Seq)
	}
	c.mu.Lock()
	inflight, stillPending := c.inflight, len(c.pending)
	c.mu.Unlock()
	if inflight != nil || stillPending == 0 {
		t.Fatalf("oversized batch admission state inflight=%v pending=%d; want unreserved pending input", inflight, stillPending)
	}
}

func TestPublisherIdleLeaseRenewalsAvoidBaseAndChunkWrites(t *testing.T) {
	b := durabilityBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	seed, _, _, _ := designFixtureState()
	if len(seed.Stories) != 5000 || len(seed.Links) != 20000 || len(seed.Rows) != 1500 || len(seed.Revisions) != 29500 {
		t.Fatalf("incorrect design fixture dimensions: stories=%d links=%d rows=%d revisions=%d", len(seed.Stories), len(seed.Links), len(seed.Rows), len(seed.Revisions))
	}
	publisherKV, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		t.Fatal(err)
	}
	seedStore := publication.New(publisherKV, "idle-lease-fixture", publisherLease, b.NC().MaxPayload())
	if err := seedStore.Initialize(ctx, seed, 0, nil); err != nil {
		t.Fatalf("seed full publication state: %v", err)
	}
	if err := seedStore.Release(ctx); err != nil {
		t.Fatal(err)
	}
	agent, err := NewPublisher(b, NewMetrics(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	p := agent.(*publisherAgent)
	if err := p.core.restore(ctx); err != nil {
		t.Fatalf("restore full fixture: %v", err)
	}
	defer p.core.release(context.Background())

	keysBefore, err := publisherKV.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rootBefore, _, err := p.core.storage.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	times := make([]time.Duration, 0, 100)
	baseEncodes, journalEncodes, chunkWrites := 0, 0, 0
	p.core.storage.Observe = func(op string, elapsed time.Duration, n int) {
		switch op {
		case "lease":
			times = append(times, elapsed)
		case "base_encode":
			baseEncodes++
		case "journal_encode":
			journalEncodes++
		case "chunk_write":
			chunkWrites++
		}
	}
	for i := 0; i < 100; i++ {
		if err := p.core.storage.Renew(ctx); err != nil {
			t.Fatalf("idle renewal %d: %v", i, err)
		}
	}
	keysAfter, err := publisherKV.Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rootAfter, _, err := p.core.storage.ReadRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(times) != 100 {
		t.Fatalf("observed %d renewals, want 100", len(times))
	}
	if !equalStringSlices(keysBefore, keysAfter) {
		t.Fatalf("idle lease renewal changed publisher bucket keys: before=%d after=%d", len(keysBefore), len(keysAfter))
	}
	if rootBefore.Base.Blob != rootAfter.Base.Blob || len(rootBefore.Entries) != len(rootAfter.Entries) || rootBefore.Head != rootAfter.Head {
		t.Fatalf("idle renewal changed base/journal: base %v -> %v, entries %d -> %d, head %d -> %d", rootBefore.Base.Blob, rootAfter.Base.Blob, len(rootBefore.Entries), len(rootAfter.Entries), rootBefore.Head, rootAfter.Head)
	}
	if baseEncodes != 0 || journalEncodes != 0 || chunkWrites != 0 {
		t.Fatalf("idle renewal performed expensive writes: base encode=%d journal encode=%d chunk writes=%d", baseEncodes, journalEncodes, chunkWrites)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	p50 := times[len(times)/2]
	p95 := times[int(math.Ceil(float64(len(times))*.95))-1]
	t.Logf("idle full-state lease renewal n=%d p50=%s p95=%s max=%s; base=%d stories/%d links/%d wire rows/%d revisions; keys=%d unchanged", len(times), p50, p95, times[len(times)-1], len(seed.Stories), len(seed.Links), len(seed.Rows), len(seed.Revisions), len(keysBefore))
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
