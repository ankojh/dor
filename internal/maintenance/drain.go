package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/publication"
)

// StreamPosition is one stream's retained bounds.
type StreamPosition struct {
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
}

// ConsumerPosition is one durable consumer's acknowledgement state. An AckNone
// consumer is recorded but cannot provide a delivery barrier.
type ConsumerPosition struct {
	Stream     string `json:"stream"`
	Name       string `json:"name"`
	Pending    uint64 `json:"pending"`
	AckPending uint64 `json:"ack_pending"`
	AckFloor   uint64 `json:"ack_floor"`
	AckPolicy  string `json:"ack_policy"`
}

// WriterCounters is the DB writer queue/flush barrier. The maintenance adapter
// receives it from the in-process writer; a nil source fails the barrier closed
// instead of assuming an empty asynchronous mirror.
type WriterCounters struct {
	Verified       bool  `json:"verified"`
	Enqueued       int64 `json:"enqueued"`
	Pending        int   `json:"pending"`
	PendingStories int   `json:"pending_stories"`
	AuxQueue       int   `json:"aux_queue"`
	Unsettled      int64 `json:"unsettled"`
	PendingEnrich  int64 `json:"pending_enrichments"`
	PendingAssign  int   `json:"pending_assignments"`
	Flushing       int64 `json:"flushing"`
	Blocked        bool  `json:"blocked"`
	DroppedQueue   int64 `json:"dropped_queue"`
	DroppedBatch   int64 `json:"dropped_batch"`
	DroppedStories int64 `json:"dropped_stories"`
	Errors         int64 `json:"errors"`
}

// DrainReport is a machine-readable quiesce sample. Stable is only true for a
// second identical sample taken after the first, with every barrier satisfied.
type DrainReport struct {
	At                int64                     `json:"at"`
	Streams           map[string]StreamPosition `json:"streams"`
	Consumers         []ConsumerPosition        `json:"consumers"`
	Producers         []ProducerLease           `json:"producers,omitempty"`
	ExpectedProducers []string                  `json:"expected_producers,omitempty"`
	ExpectedConsumers []string                  `json:"expected_consumers,omitempty"`
	JoinsPending      int                       `json:"joins_pending"`
	PublisherOutbox   bool                      `json:"publisher_outbox"`
	PublisherOwner    string                    `json:"publisher_owner,omitempty"`
	PublisherLeaseMS  int64                     `json:"publisher_lease_ms,omitempty"`
	DeltaTail         uint64                    `json:"delta_tail"`
	Writer            *WriterCounters           `json:"writer,omitempty"`
	WriterVerified    bool                      `json:"writer_verified"`
	Stable            bool                      `json:"stable"`
	Reasons           []string                  `json:"reasons,omitempty"`
}

// DrainOptions configures VerifyQuiesce.
type DrainOptions struct {
	Previous          *DrainReport
	Writer            func() WriterCounters
	Producers         []ProducerLease
	ExpectedProducers []string
	ExpectedConsumers []string
	Now               func() time.Time
}

// VerifyQuiesce samples every barrier required by the accepted cutover
// boundary: retained stream tails, all durable consumers, pending durable joins,
// the publisher outbox/owner, the DELTA tail and the DB writer queue. Queued
// input is never purged or acknowledged by maintenance.
func VerifyQuiesce(ctx context.Context, b *bus.Bus, opts DrainOptions) (*DrainReport, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	report := &DrainReport{At: now().UnixNano(), Streams: map[string]StreamPosition{}}
	for _, name := range []string{bus.StreamIngest, bus.StreamEnrich, bus.StreamStory, bus.StreamAlert, bus.StreamDelta} {
		stream, err := b.StreamNamed(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("maintenance: stream %s unavailable: %w", name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return nil, err
		}
		report.Streams[name] = StreamPosition{First: info.State.FirstSeq, Last: info.State.LastSeq}
		if name == bus.StreamDelta {
			report.DeltaTail = info.State.LastSeq
		}
		names, err := consumerNames(ctx, stream)
		if err != nil {
			return nil, err
		}
		sort.Strings(names)
		for _, consumerName := range names {
			consumer, err := stream.Consumer(ctx, consumerName)
			if err != nil {
				return nil, err
			}
			cinfo, err := consumer.Info(ctx)
			if err != nil {
				return nil, err
			}
			policy := "explicit"
			switch cinfo.Config.AckPolicy {
			case jetstream.AckNonePolicy:
				policy = "none"
			case jetstream.AckAllPolicy:
				policy = "all"
			}
			report.Consumers = append(report.Consumers, ConsumerPosition{
				Stream:     name,
				Name:       consumerName,
				Pending:    cinfo.NumPending,
				AckPending: uint64(cinfo.NumAckPending),
				AckFloor:   cinfo.AckFloor.Stream,
				AckPolicy:  policy,
			})
		}
	}
	joins, err := b.KV(ctx, bus.KVBucketJoins)
	if err != nil {
		return nil, err
	}
	lister, err := joins.ListKeysFiltered(ctx, "join.*")
	if err != nil {
		return nil, err
	}
	for range lister.Keys() {
		report.JoinsPending++
	}
	_ = lister.Stop()

	publisherKV, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return nil, err
	}
	store := publication.New(publisherKV, "", 0, b.NC().MaxPayload())
	if root, _, err := store.ReadRoot(ctx); err == nil {
		report.PublisherOutbox = root.Pending != nil
		report.PublisherOwner = root.Owner
		if root.LeaseUntil > 0 {
			report.PublisherLeaseMS = time.Until(time.Unix(0, root.LeaseUntil)).Milliseconds()
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, err
	}
	if opts.Writer != nil {
		counters := opts.Writer()
		report.Writer = &counters
		report.WriterVerified = true
	}
	report.Producers = append(report.Producers, opts.Producers...)
	report.ExpectedProducers = append(report.ExpectedProducers, opts.ExpectedProducers...)
	report.ExpectedConsumers = append(report.ExpectedConsumers, opts.ExpectedConsumers...)

	report.Reasons = quiesceReasons(report)
	if prev := opts.Previous; prev != nil {
		compareQuiesce(report, prev)
	} else {
		report.Reasons = append(report.Reasons, "stability window requires a second sample")
	}
	report.Stable = opts.Previous != nil && len(report.Reasons) == 0
	return report, nil
}

// consumerNames drains the paged consumer-name lister.
func consumerNames(ctx context.Context, stream jetstream.Stream) ([]string, error) {
	lister := stream.ConsumerNames(context.WithoutCancel(ctx))
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func quiesceReasons(report *DrainReport) []string {
	var reasons []string
	for _, c := range report.Consumers {
		if c.AckPolicy == "none" {
			continue
		}
		if c.Pending > 0 {
			reasons = append(reasons, fmt.Sprintf("consumer %s/%s has %d pending", c.Stream, c.Name, c.Pending))
		}
		if c.AckPending > 0 {
			reasons = append(reasons, fmt.Sprintf("consumer %s/%s has %d ack-pending", c.Stream, c.Name, c.AckPending))
		}
	}
	if report.JoinsPending > 0 {
		reasons = append(reasons, fmt.Sprintf("%d durable joins pending", report.JoinsPending))
	}
	if len(report.ExpectedConsumers) == 0 {
		reasons = append(reasons, "expected consumer inventory is required")
	} else {
		present := map[string]bool{}
		for _, c := range report.Consumers {
			present[c.Stream+"/"+c.Name] = true
		}
		for _, id := range report.ExpectedConsumers {
			if !present[id] {
				reasons = append(reasons, "expected consumer "+id+" is missing")
			}
		}
		expected := map[string]bool{}
		for _, id := range report.ExpectedConsumers {
			expected[id] = true
		}
		for _, c := range report.Consumers {
			if c.AckPolicy == "none" {
				continue
			}
			if id := c.Stream + "/" + c.Name; !expected[id] {
				reasons = append(reasons, "unexpected durable consumer "+id+" is not in the inventory")
			}
		}
	}
	if len(report.ExpectedProducers) == 0 {
		reasons = append(reasons, "expected producer inventory is required")
	} else {
		byID := map[string]ProducerLease{}
		for _, lease := range report.Producers {
			byID[lease.ID] = lease
		}
		for _, id := range report.ExpectedProducers {
			lease, ok := byID[id]
			if !ok {
				reasons = append(reasons, "producer "+id+" has no live lease")
				continue
			}
			if !lease.Healthy {
				reasons = append(reasons, "producer "+id+" barrier is unhealthy: "+lease.Error)
				continue
			}
			if !lease.IngressClosed || lease.IngestInflight > 0 {
				reasons = append(reasons, "producer "+id+" ingress is not closed")
			}
			if !lease.Writer.Verified {
				reasons = append(reasons, "producer "+id+" has no verified writer barrier")
			} else if writerWork(lease.Writer) > 0 {
				reasons = append(reasons, "producer "+id+" writer work is not settled")
			}
		}
	}
	if len(report.Producers) == 0 && !report.WriterVerified {
		reasons = append(reasons, "DB writer barrier is unavailable")
	} else if report.Writer != nil {
		if !report.Writer.Verified {
			reasons = append(reasons, "DB writer barrier is unavailable")
		}
		if report.Writer.Pending > 0 || report.Writer.PendingStories > 0 || report.Writer.AuxQueue > 0 || report.Writer.Unsettled > 0 {
			reasons = append(reasons, "DB writer queue/in-flight work is not empty")
		}
		if report.Writer.PendingEnrich > 0 || report.Writer.PendingAssign > 0 {
			reasons = append(reasons, "DB writer retry work is not empty")
		}
		if report.Writer.Flushing > 0 {
			reasons = append(reasons, "DB writer is currently flushing")
		}
		if report.Writer.DroppedQueue > 0 || report.Writer.DroppedBatch > 0 || report.Writer.DroppedStories > 0 {
			reasons = append(reasons, "DB writer reported terminal drops")
		}
	}
	return reasons
}

func compareQuiesce(current, previous *DrainReport) {
	for name, pos := range current.Streams {
		before, ok := previous.Streams[name]
		if !ok {
			current.Reasons = append(current.Reasons, "stream "+name+" was not present in the prior sample")
			continue
		}
		if before.First != pos.First {
			current.Reasons = append(current.Reasons, "retention advanced on "+name+" during the stability window")
		}
		if before.Last != pos.Last {
			current.Reasons = append(current.Reasons, "stream "+name+" tail moved during the stability window")
		}
	}
	prevConsumers := map[string]ConsumerPosition{}
	for _, c := range previous.Consumers {
		prevConsumers[c.Stream+"/"+c.Name] = c
	}
	currentConsumers := map[string]bool{}
	for _, c := range current.Consumers {
		currentConsumers[c.Stream+"/"+c.Name] = true
		before, ok := prevConsumers[c.Stream+"/"+c.Name]
		if !ok {
			current.Reasons = append(current.Reasons, "consumer "+c.Stream+"/"+c.Name+" appeared during the stability window")
			continue
		}
		if before.Pending != c.Pending || before.AckPending != c.AckPending || before.AckFloor != c.AckFloor {
			current.Reasons = append(current.Reasons, "consumer "+c.Stream+"/"+c.Name+" position moved during the stability window")
		}
	}
	for key := range prevConsumers {
		if !currentConsumers[key] {
			current.Reasons = append(current.Reasons, "consumer "+key+" disappeared during the stability window")
		}
	}
	prevProducers := map[string]ProducerLease{}
	for _, lease := range previous.Producers {
		prevProducers[lease.ID] = lease
	}
	for _, lease := range current.Producers {
		if before, ok := prevProducers[lease.ID]; ok {
			if before.IngestLast != lease.IngestLast || before.Paused != lease.Paused || before.IngestInflight != lease.IngestInflight || before.Healthy != lease.Healthy || before.IngressClosed != lease.IngressClosed || before.DownstreamInflight != lease.DownstreamInflight {
				current.Reasons = append(current.Reasons, "producer "+lease.ID+" state moved during the stability window")
			}
		}
	}
	if previous.Writer != nil && current.Writer != nil {
		if previous.Writer.Enqueued != current.Writer.Enqueued && previous.Writer.Pending == 0 && current.Writer.Pending == 0 {
			// A quiet enqueue counter may legitimately grow while the queue
			// drains to zero; only the pending depth is a barrier.
			_ = previous
		}
		if previous.Writer.Enqueued > current.Writer.Enqueued {
			current.Reasons = append(current.Reasons, "DB writer enqueue counter regressed")
		}
	}
}

// SettlePending publishes an uncertain reserved outbox exactly as recorded and
// commits it through the normal store watermark. It never edits, discards,
// renumbers or advances the outbox by inspection.
func SettlePending(ctx context.Context, b *bus.Bus, store *publication.Store) (bool, error) {
	root, _, err := store.ReadRoot(ctx)
	if err != nil {
		return false, err
	}
	if root.Pending == nil {
		return false, nil
	}
	state, pending, err := store.Load(ctx)
	if err != nil {
		return false, err
	}
	if pending == nil {
		return false, errors.New("maintenance: root advertises a missing pending record")
	}
	_ = state
	last, raw, err := pending.End()
	if err != nil {
		return false, err
	}
	if len(pending.Data) > 0 {
		if err := publishExact(ctx, b, root.Raw, pending.Data, fmt.Sprintf("delta:%d", last)); err != nil {
			return false, err
		}
	}
	if err := store.Commit(ctx, pending.Ordinal, last, raw); err != nil {
		return false, err
	}
	return true, nil
}

// publishExact mirrors the publisher's durable publication path: stream-level
// expected-last sequence plus exact-byte reconciliation after an ambiguous
// PubAck. A lost acknowledgement is never treated as an invitation to skip.
func publishExact(ctx context.Context, b *bus.Bus, expectedRaw uint64, data []byte, msgID string) error {
	header := nats.Header{}
	header.Set(jetstream.ExpectedLastSeqHeader, fmt.Sprint(expectedRaw))
	_, err := b.Publish(ctx, bus.SubjectDeltaOut, data, bus.WithMsgID(msgID), bus.WithHeaders(header))
	if err == nil {
		return nil
	}
	stream, getErr := b.StreamNamed(ctx, bus.StreamDelta)
	if getErr != nil {
		return err
	}
	msg, getErr := stream.GetMsg(ctx, expectedRaw+1)
	if getErr == nil && bytes.Equal(msg.Data, data) {
		return nil
	}
	if errors.Is(getErr, jetstream.ErrMsgNotFound) {
		info, infoErr := stream.Info(ctx)
		if infoErr == nil && info.State.LastSeq == expectedRaw+1 {
			return nil
		}
	}
	return err
}
