package maintenance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/ankojh/dor/internal/publication"
)

const maintenanceLease = 1200 * time.Millisecond

// Coordinator serialises the cross-store maintenance phases under the existing
// publisher owner lease. It never resets source streams or consumer positions.
type Coordinator struct {
	bus         *bus.Bus
	kv          jetstream.KeyValue
	leases      jetstream.KeyValue
	producers   jetstream.KeyValue
	authority   ExclusionAuthority
	store       *publication.Store
	now         func() time.Time
	environment string
}

// NewCoordinator binds the non-expiring publisher bucket, the short-lived
// gateway lease bucket and the immutable isolated-environment identity.
func NewCoordinator(ctx context.Context, b *bus.Bus, environmentID string) (*Coordinator, error) {
	if b == nil {
		return nil, errors.New("maintenance: bus is required")
	}
	environment, err := NormalizeEnvironmentID(environmentID)
	if err != nil {
		return nil, err
	}
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return nil, err
	}
	leases, err := b.KV(ctx, bus.KVBucketGatewayLease)
	if err != nil {
		return nil, err
	}
	producers, err := b.KV(ctx, bus.KVBucketProducerLease)
	if err != nil {
		return nil, err
	}
	return &Coordinator{bus: b, kv: kv, leases: leases, producers: producers, now: time.Now, environment: environment}, nil
}

// validateEnvironment proves that the coordinator, publisher store and every
// current participant lease belong to the same explicit environment. It is
// read-only and is called before the first fence or write of every run.
func (c *Coordinator) validateEnvironment(ctx context.Context, requested string) error {
	want, err := NormalizeEnvironmentID(requested)
	if err != nil {
		return err
	}
	if c.environment == "" {
		return errors.New("maintenance: coordinator environment identity is unbound")
	}
	if c.environment != want {
		return fmt.Errorf("maintenance: coordinator environment %q does not match requested %q", c.environment, want)
	}
	env, revision, err := ReadEnvironment(ctx, c.kv)
	if err != nil {
		return err
	}
	if revision == 0 {
		return errors.New("maintenance: publisher environment identity is unbound; run the explicit bind setup first")
	}
	if err := RequireEnvironment("publisher", env.ID, want); err != nil {
		return err
	}
	return ValidateParticipantEnvironments(ctx, c.leases, c.producers, want)
}

// BeginGate closes gateway ingress before any drain. It is idempotent and
// deliberately does not require a plan: the fleet must be excluded even if the
// process stops before planning.
func (c *Coordinator) BeginGate(ctx context.Context, reason string) error {
	if err := c.validateEnvironment(ctx, c.environment); err != nil {
		return err
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if control.Version == 0 {
		control = Control{Version: ControlVersion}
	}
	fresh := !control.Blocked()
	if control.FenceActive && control.Required {
		return nil
	}
	if fresh {
		// A completed run leaves the serving phase and its run id for audit.
		// A new maintenance run starts a fresh journal; the old plan/floor stay
		// non-expiring.
		control.RunID = ""
		control.PlanHash = ""
		control.FleetEpoch++
		control.Phase = PhaseDraining
		control.IngressClosed = true
		control.DownstreamFenced = false
		control.WriterClosed = false
		control.DeliveryFenced = false
		control.DeliveryConsumers = nil
		if err := ResetJournal(ctx, c.kv); err != nil {
			return err
		}
	}
	control.FenceActive = true
	control.Required = true
	control.IngressClosed = true
	control.Phase = PhaseDraining
	control.UpdatedAt = c.now().UnixNano()
	if reason != "" {
		control.LastError = ""
	}
	return UpdateControl(ctx, c.kv, control)
}

// AwaitFleetExclusion waits for floor-aware gateways to acknowledge the fence.
func (c *Coordinator) AwaitFleetExclusion(ctx context.Context, proof ExclusionProof, timeout time.Duration) error {
	if err := c.validateEnvironment(ctx, c.environment); err != nil {
		return err
	}
	if c.leases == nil {
		return errors.New("maintenance: gateway lease bucket unavailable")
	}
	if err := ValidateExclusionProof(proof); err != nil {
		return err
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	control.ExclusionAuthority = proof.Authority
	control.ExclusionEvidenceID = proof.EvidenceID
	control.ExclusionGeneration = proof.Generation
	control.ExclusionDigest = proof.Digest
	control.UpdatedAt = c.now().UnixNano()
	if err := UpdateControl(ctx, c.kv, control); err != nil {
		return err
	}
	return AwaitGatewayExclusion(ctx, c.leases, proof.Expected, proof.Excluded, timeout)
}

// AwaitProducerAdmission waits for the staged ingress closure only. Downstream
// consumers and writers remain active so the finite backlog can drain.
func (c *Coordinator) AwaitProducerAdmission(ctx context.Context, expected, excluded []string, timeout time.Duration) ([]ProducerLease, error) {
	if c.producers == nil {
		return nil, errors.New("maintenance: producer lease bucket unavailable")
	}
	return AwaitProducerAdmission(ctx, c.producers, expected, excluded, timeout)
}

// AwaitProducerFence waits for the full downstream closure and in-flight bar.
func (c *Coordinator) AwaitProducerFence(ctx context.Context, expected, excluded []string, timeout time.Duration) ([]ProducerLease, error) {
	if c.producers == nil {
		return nil, errors.New("maintenance: producer lease bucket unavailable")
	}
	return AwaitProducerFence(ctx, c.producers, expected, excluded, timeout)
}

// AwaitProducerWriterClosed waits for the final DB writer admission closure.
func (c *Coordinator) AwaitProducerWriterClosed(ctx context.Context, expected, excluded []string, timeout time.Duration) ([]ProducerLease, error) {
	if c.producers == nil {
		return nil, errors.New("maintenance: producer lease bucket unavailable")
	}
	return AwaitProducerWriterClosed(ctx, c.producers, expected, excluded, timeout)
}

// PauseConsumers fences new handler/delivery admission for every expected
// durable consumer. In-flight handlers remain unacknowledged until they finish;
// no message is reset, purged or acknowledged by maintenance.
func (c *Coordinator) PauseConsumers(ctx context.Context, expected []string) error {
	if err := c.validateEnvironment(ctx, c.environment); err != nil {
		return err
	}
	if len(expected) == 0 {
		return errors.New("maintenance: expected consumer inventory is required")
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if control.DeliveryFenced && len(control.DeliveryConsumers) > 0 && !sameStringSet(control.DeliveryConsumers, expected) {
		return errors.New("maintenance: consumer inventory changed during the fenced run")
	}
	until := c.now().Add(24 * time.Hour)
	for _, id := range expected {
		streamName, name, err := splitConsumerID(id)
		if err != nil {
			return err
		}
		stream, err := c.bus.StreamNamed(ctx, streamName)
		if err != nil {
			return err
		}
		if _, err := stream.PauseConsumer(ctx, name, until); err != nil {
			return fmt.Errorf("maintenance: pause consumer %s: %w", id, err)
		}
	}
	control, _, err = ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	control.DeliveryFenced = true
	control.DeliveryConsumers = append([]string(nil), expected...)
	control.UpdatedAt = c.now().UnixNano()
	return UpdateControl(ctx, c.kv, control)
}

// ResumeConsumers opens delivery after the corrected policy/floor are durable
// and immediately before fleet admission.
func (c *Coordinator) ResumeConsumers(ctx context.Context, expected []string) error {
	for _, id := range expected {
		streamName, name, err := splitConsumerID(id)
		if err != nil {
			return err
		}
		stream, err := c.bus.StreamNamed(ctx, streamName)
		if err != nil {
			return err
		}
		if _, err := stream.ResumeConsumer(ctx, name); err != nil {
			return fmt.Errorf("maintenance: resume consumer %s: %w", id, err)
		}
	}
	return nil
}

func splitConsumerID(id string) (string, string, error) {
	streamName, name, ok := strings.Cut(id, "/")
	if !ok || streamName == "" || name == "" {
		return "", "", fmt.Errorf("maintenance: consumer inventory entry %q must be STREAM/name", id)
	}
	return streamName, name, nil
}

// AwaitConsumerQuiescence waits for every expected consumer to be server-side
// paused with no pending or in-flight handler acknowledgements.
func (c *Coordinator) AwaitConsumerQuiescence(ctx context.Context, expected []string, timeout time.Duration) error {
	if len(expected) == 0 {
		return errors.New("maintenance: expected consumer inventory is required")
	}
	deadline := c.now().Add(timeout)
	for {
		blocked := []string{}
		for _, id := range expected {
			streamName, name, err := splitConsumerID(id)
			if err != nil {
				return err
			}
			stream, err := c.bus.StreamNamed(ctx, streamName)
			if err != nil {
				blocked = append(blocked, id+" missing")
				continue
			}
			consumer, err := stream.Consumer(ctx, name)
			if err != nil {
				blocked = append(blocked, id+" missing")
				continue
			}
			info, err := consumer.Info(ctx)
			if err != nil {
				blocked = append(blocked, id+" unreadable")
				continue
			}
			if !info.Paused || info.NumPending > 0 || info.NumAckPending > 0 {
				blocked = append(blocked, fmt.Sprintf("%s paused=%t pending=%d ack=%d", id, info.Paused, info.NumPending, info.NumAckPending))
			}
		}
		if len(blocked) == 0 {
			return nil
		}
		if c.now().After(deadline) {
			return fmt.Errorf("maintenance: consumer delivery not quiescent: %v", blocked)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// RecheckAfterDeliveryFence re-samples consumers, joins, DB writer and producer
// leases after handler admission is closed.
func (c *Coordinator) RecheckAfterDeliveryFence(ctx context.Context, req *ApplyRequest, timeout time.Duration) error {
	if err := c.AwaitConsumerQuiescence(ctx, req.ExpectedConsumers, timeout); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	var previous *DrainReport
	for {
		producers, err := c.AwaitProducerAdmission(ctx, req.ExpectedProducers, req.ExcludedProducers, 10*time.Second)
		if err != nil {
			return err
		}
		opts := req.Drain
		opts.Producers = producers
		opts.ExpectedProducers = req.ExpectedProducers
		opts.ExpectedConsumers = req.ExpectedConsumers
		opts.Previous = previous
		report, err := VerifyQuiesce(ctx, c.bus, opts)
		if err != nil {
			return err
		}
		if report.Stable {
			control, _, cerr := ReadControl(ctx, c.kv)
			if cerr != nil {
				return cerr
			}
			return Advance(ctx, c.kv, control.RunID, PhaseDeliveryFenced, map[string]string{"consumers": fmt.Sprint(len(req.ExpectedConsumers))}, "handler delivery fenced and rechecked", c.now())
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("maintenance: post-handler-fence sample not stable: %v", report.Reasons)
		}
		previous = report
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// CloseDownstream fences every downstream writer after the finite backlog
// drain, before the publisher root is claimed. It does not require a plan.
func (c *Coordinator) CloseDownstream(ctx context.Context) error {
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if !control.Blocked() {
		return ErrFenced
	}
	control.IngressClosed = true
	control.DownstreamFenced = true
	control.UpdatedAt = c.now().UnixNano()
	return UpdateControl(ctx, c.kv, control)
}

// CloseWriterAdmission closes new DB writer admissions after the downstream
// in-flight operation barrier is empty. Existing accepted work stays queued.
func (c *Coordinator) CloseWriterAdmission(ctx context.Context) error {
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if !control.Blocked() || !control.DownstreamShut() {
		return ErrFenced
	}
	control.WriterClosed = true
	control.UpdatedAt = c.now().UnixNano()
	return UpdateControl(ctx, c.kv, control)
}

// FenceForRun binds the durable control and journal to one deterministic plan
// after the drain and publisher claim. A different unfinished run is refused.
func (c *Coordinator) FenceForRun(ctx context.Context, runID string) error {
	if runID == "" {
		return errors.New("maintenance: empty run id")
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if !control.Blocked() {
		return ErrFenced
	}
	if control.RunID != "" && control.RunID != runID {
		return fmt.Errorf("maintenance: run %s is already active", control.RunID)
	}
	control.RunID = runID
	control.Phase = PhaseFenced
	control.Required = true
	control.FenceActive = true
	control.IngressClosed = true
	control.DownstreamFenced = true
	control.WriterClosed = true
	control.UpdatedAt = c.now().UnixNano()
	control.LastError = ""
	if err := UpdateControl(ctx, c.kv, control); err != nil {
		return err
	}
	return Advance(ctx, c.kv, runID, PhaseFenced, map[string]string{"fence": "active"}, "gateways excluded and writers fenced", c.now())
}

// ClaimPublisher acquires the exclusive publisher root lease. A live publisher
// must have been stopped first; this waits at most the lease duration.
func (c *Coordinator) ClaimPublisher(ctx context.Context) error {
	if err := c.validateEnvironment(ctx, c.environment); err != nil {
		return err
	}
	c.store = publication.New(c.kv, nuid.Next(), maintenanceLease, c.bus.NC().MaxPayload())
	deadline := c.now().Add(maintenanceLease + 200*time.Millisecond)
	for {
		err := c.store.Claim(ctx)
		if !errors.Is(err, publication.ErrOwned) {
			return err
		}
		if c.now().After(deadline) {
			return errors.New("maintenance: publisher root is still owned by a live writer")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Release drops the maintenance owner claim while preserving every durable
// record.
func (c *Coordinator) Release(ctx context.Context) error {
	if c.store == nil {
		return nil
	}
	err := c.store.Release(ctx)
	if err == nil {
		c.store = nil
	}
	return err
}

// LoadState reads the committed publication state under the maintenance lease.
func (c *Coordinator) LoadState(ctx context.Context) (*publication.State, error) {
	if c.store == nil {
		return nil, errors.New("maintenance: publisher not claimed")
	}
	state, pending, err := c.store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		return nil, errors.New("maintenance: pending outbox must be settled before state repair")
	}
	return state, nil
}

// SettleOutbox settles an uncertain pending publication unchanged.
func (c *Coordinator) SettleOutbox(ctx context.Context) (bool, error) {
	if c.store == nil {
		return false, errors.New("maintenance: publisher not claimed")
	}
	return SettlePending(ctx, c.bus, c.store)
}

// PersistPolicy writes the durable per-article policy and journals the phase.
func (c *Coordinator) PersistPolicy(ctx context.Context, plan *Plan) (*WireTimePolicy, error) {
	if err := plan.Verify(); err != nil {
		return nil, err
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return nil, err
	}
	if control.RunID != plan.RunID || !control.FenceActive {
		return nil, errors.New("maintenance: policy write requires the active fenced run")
	}
	policy := plan.Policy()
	hash, err := PutPolicy(ctx, c.kv, policy)
	if err != nil {
		return nil, err
	}
	if hash != plan.PolicyHash {
		return nil, errors.New("maintenance: policy hash does not match plan")
	}
	SetCurrentPolicy(policy)
	if err := Advance(ctx, c.kv, plan.RunID, PhasePolicyPersisted, map[string]string{"policy_hash": hash}, "durable wire-time policy persisted", c.now()); err != nil {
		return nil, err
	}
	return policy, nil
}

// ForwardReport records the forward authority repair result.
type ForwardReport struct {
	TargetSeq uint64 `json:"target_seq"`
	RawSeq    uint64 `json:"raw_seq"`
	Records   int    `json:"records"`
	Changed   int    `json:"changed"`
	Skipped   int    `json:"skipped"`
	Absent    int    `json:"absent"`
}

// RepairRetained writes forward-only wire tombstones/corrections through the
// normal durable outbox/commit path. Records are never edited or renumbered;
// each batch gets a fresh contiguous global sequence range and a fresh raw
// DELTA stream position.
func (c *Coordinator) RepairRetained(ctx context.Context, plan *Plan, policy *WireTimePolicy) (*ForwardReport, error) {
	state, err := c.LoadState(ctx)
	if err != nil {
		return nil, err
	}
	if err := checkPlanProgress(plan, policy, state); err != nil {
		return nil, err
	}
	// Before the first forward record, also verify the deterministic plan is
	// still bound to the actual publisher root and live-snapshot revisions. A
	// partially repaired run has necessarily advanced the root, so the per-row
	// before hashes above are the binding for that phase.
	if !repairStarted(plan, policy, state) {
		if err := BindPlanToCurrentRoot(ctx, c.bus, plan); err != nil {
			return nil, err
		}
	}
	report := &ForwardReport{}
	var deltas []*dorv1.Delta
	next := state.Seq
	seen := map[string]bool{}
	for _, item := range plan.Articles {
		if seen[item.ArticleID] {
			continue
		}
		seen[item.ArticleID] = true
		row := state.Rows[item.ArticleID]
		if row == nil {
			report.Absent++
			continue
		}
		target := ApplyPolicy(policy, item.ArticleID, row.At)
		if row.At == target {
			report.Skipped++
			continue
		}
		clone := proto.Clone(row).(*dorv1.WireRow)
		clone.At = target
		next++
		cloneDelta := &dorv1.Delta{Seq: next, Body: &dorv1.Delta_Wire{Wire: clone}}
		deltas = append(deltas, cloneDelta)
	}
	if len(deltas) == 0 {
		report.TargetSeq, report.RawSeq = state.Seq, state.Raw
		if err := Advance(ctx, c.kv, plan.RunID, PhaseForwardCommitted, seqValues(report.TargetSeq, report.RawSeq, "0"), "no retained row required a forward correction", c.now()); err != nil {
			return nil, err
		}
		return report, nil
	}
	for start := 0; start < len(deltas); {
		batch := &dorv1.DeltaBatch{}
		for start < len(deltas) {
			candidate := append(batch.Deltas, deltas[start])
			data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&dorv1.DeltaBatch{Deltas: candidate})
			if err != nil {
				return nil, err
			}
			if int64(len(data))+1024 > c.bus.NC().MaxPayload() && len(batch.Deltas) > 0 {
				break
			}
			if len(batch.Deltas) == 0 && int64(len(data))+1024 > c.bus.NC().MaxPayload() {
				return nil, errors.New("maintenance: one wire correction exceeds the DELTA payload limit")
			}
			batch = &dorv1.DeltaBatch{Deltas: candidate}
			start++
		}
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(batch)
		if err != nil {
			return nil, err
		}
		if int64(len(data))+1024 > c.bus.NC().MaxPayload() {
			return nil, errors.New("maintenance: repair batch exceeds the DELTA payload limit")
		}
		record := &publication.Record{
			Ordinal:     state.Ordinal + 1,
			PreviousSeq: state.Seq,
			PreviousRaw: state.Raw,
			Data:        data,
			Revisions:   map[string]uint64{},
			WireOrder:   publication.OrderChange{},
			ScoreOrder:  publication.OrderChange{},
		}
		if err := c.store.Reserve(ctx, record); err != nil {
			return nil, err
		}
		last, raw, err := record.End()
		if err != nil {
			return nil, err
		}
		if err := publishExact(ctx, c.bus, record.PreviousRaw, data, fmt.Sprintf("maintenance-repair:%d:%d", record.Ordinal, last)); err != nil {
			return nil, err
		}
		if err := c.store.Commit(ctx, record.Ordinal, last, raw); err != nil {
			return nil, err
		}
		if err := state.Apply(record, raw); err != nil {
			return nil, err
		}
		report.Records++
		report.Changed += len(batch.Deltas)
	}
	report.TargetSeq, report.RawSeq = state.Seq, state.Raw
	if err := Advance(ctx, c.kv, plan.RunID, PhaseForwardCommitted, seqValues(report.TargetSeq, report.RawSeq, fmt.Sprint(report.Records)), "forward repair committed", c.now()); err != nil {
		return nil, err
	}
	return report, nil
}

func seqValues(t, d uint64, extra string) map[string]string {
	return map[string]string{"target_seq": fmt.Sprint(t), "raw_seq": fmt.Sprint(d), "records": extra}
}

// checkPlanProgress verifies the plan while tolerating a partially completed
// forward repair: repaired rows must equal the policy target; untouched rows
// must still match their recorded before hash.
func checkPlanProgress(plan *Plan, policy *WireTimePolicy, state *publication.State) error {
	for _, item := range plan.Articles {
		row := state.Rows[item.ArticleID]
		if row == nil {
			if item.Present && item.BeforeHash != "absent" {
				// The row was present at planning and has since expired. Never
				// resurrect an absent entity.
				continue
			}
			continue
		}
		target := ApplyPolicy(policy, item.ArticleID, row.At)
		if row.At == target {
			continue
		}
		if rowHash(row) != item.BeforeHash {
			return fmt.Errorf("maintenance: article %s changed outside the plan", item.ArticleID)
		}
	}
	return nil
}

// repairStarted reports whether any affected row already carries its planned
// target while differing from the before hash.
func repairStarted(plan *Plan, policy *WireTimePolicy, state *publication.State) bool {
	for _, item := range plan.Articles {
		row := state.Rows[item.ArticleID]
		if row == nil {
			continue
		}
		target := ApplyPolicy(policy, item.ArticleID, row.At)
		if row.At == target && rowHash(row) != item.BeforeHash {
			return true
		}
	}
	return false
}

// RepairHotArticles reconciles the live_state article.<id> PublishedAt fields
// that matcher backfill and story details read. The publisher policy remains
// authoritative for wire presentation; this closes the separate hot-state
// timestamp path under the same barrier.
func (c *Coordinator) RepairHotArticles(ctx context.Context, plan *Plan) (changed, skipped int, err error) {
	liveKV, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return 0, 0, err
	}
	if !hotRepairStarted(plan, liveKV, ctx) {
		if err := checkHotBindings(ctx, liveKV, plan); err != nil {
			return 0, 0, err
		}
	}
	for _, binding := range plan.HotArticles {
		entry, gerr := liveKV.Get(ctx, binding.Key)
		if errors.Is(gerr, jetstream.ErrKeyNotFound) {
			if !binding.Present {
				skipped++
				continue
			}
			// Expired/absent hot entities stay absent; never resurrect them.
			skipped++
			continue
		}
		if gerr != nil {
			return changed, skipped, gerr
		}
		var article dorv1.Article
		if uerr := proto.Unmarshal(entry.Value(), &article); uerr != nil {
			return changed, skipped, fmt.Errorf("maintenance: hot article %s: %w", binding.ArticleID, uerr)
		}
		if article.PublishedAt == binding.TargetAt {
			skipped++
			continue
		}
		if entry.Revision() != binding.Revision || "sha256:"+hashBytes(entry.Value()) != binding.BeforeHash {
			return changed, skipped, fmt.Errorf("maintenance: hot article %s changed outside the plan", binding.ArticleID)
		}
		article.PublishedAt = binding.TargetAt
		data, merr := proto.MarshalOptions{Deterministic: true}.Marshal(&article)
		if merr != nil {
			return changed, skipped, merr
		}
		if _, uerr := liveKV.Update(ctx, binding.Key, data, entry.Revision()); uerr != nil {
			return changed, skipped, uerr
		}
		changed++
	}
	if err := Advance(ctx, c.kv, plan.RunID, PhaseHotReconciled, map[string]string{"changed": fmt.Sprint(changed), "skipped": fmt.Sprint(skipped)}, "hot article timestamps reconciled", c.now()); err != nil {
		return changed, skipped, err
	}
	return changed, skipped, nil
}

func hotRepairStarted(plan *Plan, liveKV jetstream.KeyValue, ctx context.Context) bool {
	for _, binding := range plan.HotArticles {
		entry, err := liveKV.Get(ctx, binding.Key)
		if err != nil {
			continue
		}
		if entry.Revision() != binding.Revision && "sha256:"+hashBytes(entry.Value()) != binding.BeforeHash {
			var article dorv1.Article
			if proto.Unmarshal(entry.Value(), &article) == nil && article.PublishedAt == binding.TargetAt {
				return true
			}
		}
	}
	return false
}

func checkHotBindings(ctx context.Context, liveKV jetstream.KeyValue, plan *Plan) error {
	for _, binding := range plan.HotArticles {
		entry, err := liveKV.Get(ctx, binding.Key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if binding.Present {
				return fmt.Errorf("maintenance: hot article %s disappeared after planning", binding.ArticleID)
			}
			continue
		}
		if err != nil {
			return err
		}
		if !binding.Present {
			return fmt.Errorf("maintenance: hot article %s appeared after planning", binding.ArticleID)
		}
		if entry.Revision() != binding.Revision || "sha256:"+hashBytes(entry.Value()) != binding.BeforeHash {
			var article dorv1.Article
			if proto.Unmarshal(entry.Value(), &article) != nil || article.PublishedAt != binding.TargetAt {
				return fmt.Errorf("maintenance: hot article %s changed after planning", binding.ArticleID)
			}
		}
	}
	return nil
}

// WriteSnapshot materializes the corrected committed state as the public
// snapshot. Cache lag is not evidence; this rebuilds from authority.
func (c *Coordinator) WriteSnapshot(ctx context.Context, plan *Plan) (uint64, error) {
	state, err := c.LoadState(ctx)
	if err != nil {
		return 0, err
	}
	root, _, err := c.store.ReadRoot(ctx)
	if err != nil {
		return 0, err
	}
	if state.Seq != root.Seq || state.Raw != root.Raw {
		return 0, errors.New("maintenance: loaded state is not the committed root")
	}
	snap := state.Snapshot()
	data, err := c.store.CacheValue(ctx, snap)
	if err != nil {
		return 0, err
	}
	liveKV, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return 0, err
	}
	if err := writePublicSnapshot(ctx, c.store, liveKV, snap.Seq, data); err != nil {
		return 0, err
	}
	if err := Advance(ctx, c.kv, plan.RunID, PhaseSnapshot, map[string]string{"snapshot_seq": fmt.Sprint(snap.Seq), "target_seq": fmt.Sprint(root.Seq)}, "corrected snapshot committed", c.now()); err != nil {
		return 0, err
	}
	return snap.Seq, nil
}

func writePublicSnapshot(ctx context.Context, store *publication.Store, kv jetstream.KeyValue, seq uint64, data []byte) error {
	for {
		entry, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
		if err == nil {
			if prev, e := store.DecodeCache(ctx, entry.Value()); e == nil {
				if prev.Seq > seq {
					return nil
				}
				if prev.Seq == seq && len(prev.Stories) > 0 && len(data) == 0 {
					return errors.New("maintenance: refusing empty snapshot over live state")
				}
			}
			_, err = kv.Update(ctx, bus.KVLiveStateSnapshotKey, data, entry.Revision())
		} else if errors.Is(err, jetstream.ErrKeyNotFound) {
			_, err = kv.Create(ctx, bus.KVLiveStateSnapshotKey, data)
		}
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) {
			continue
		}
		return err
	}
}

// ActivateFloor publishes the monotonic gateway baseline only after the
// corrected checkpoint, empty outbox and corrected snapshot are durable.
func (c *Coordinator) ActivateFloor(ctx context.Context, plan *Plan, policyHash string) (Floor, error) {
	root, _, err := c.store.ReadRoot(ctx)
	if err != nil {
		return Floor{}, err
	}
	if root.Pending != nil {
		return Floor{}, errors.New("maintenance: cannot activate floor with a pending outbox")
	}
	if root.Seq == 0 {
		return Floor{}, errors.New("maintenance: cannot activate a zero global baseline")
	}
	liveKV, err := c.bus.KV(ctx, bus.KVBucketLiveState)
	if err != nil {
		return Floor{}, err
	}
	entry, err := liveKV.Get(ctx, bus.KVLiveStateSnapshotKey)
	if err != nil {
		return Floor{}, fmt.Errorf("maintenance: corrected snapshot unavailable: %w", err)
	}
	snap, err := c.store.DecodeCache(ctx, entry.Value())
	if err != nil {
		return Floor{}, err
	}
	if snap.Seq < root.Seq {
		return Floor{}, errors.New("maintenance: snapshot is behind the repaired checkpoint")
	}
	floor := Floor{
		Version:            ControlVersion,
		Initialized:        true,
		MinimumResumeSeq:   root.Seq,
		BootstrapStreamSeq: root.Raw,
		RepairID:           plan.RunID,
		RunID:              plan.RunID,
		PolicyHash:         policyHash,
		RequestID:          plan.RequestID,
		WireTombstone:      true,
		ActivatedAt:        c.now().UnixNano(),
	}
	if err := UpdateFloor(ctx, c.kv, floor); err != nil {
		return Floor{}, err
	}
	activation, arev, err := ReadActivation(ctx, c.kv)
	if err != nil {
		return Floor{}, err
	}
	if activation == nil {
		arev = 0
	}
	if _, err := writeJSON(ctx, c.kv, KeyActivated, Activation{Version: ControlVersion, RunID: plan.RunID, GlobalSeq: root.Seq, RawSeq: root.Raw, Activated: c.now().UnixNano()}, arev); err != nil {
		return Floor{}, err
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return Floor{}, err
	}
	if control.RunID != plan.RunID {
		return Floor{}, errors.New("maintenance: control run changed before floor activation")
	}
	control.Required = true
	control.FenceActive = true
	control.Phase = PhaseFloorActivated
	control.PlanHash = plan.Hash()
	control.GlobalSeq = root.Seq
	control.RawSeq = root.Raw
	control.UpdatedAt = c.now().UnixNano()
	if err := UpdateControl(ctx, c.kv, control); err != nil {
		return Floor{}, err
	}
	if err := Advance(ctx, c.kv, plan.RunID, PhaseFloorActivated, map[string]string{"global_seq": fmt.Sprint(root.Seq), "raw_seq": fmt.Sprint(root.Raw), "snapshot_seq": fmt.Sprint(snap.Seq)}, "gateway baseline activated", c.now()); err != nil {
		return Floor{}, err
	}
	return floor, nil
}

// runDBPhase makes the DB transaction and the journal crash-consistent. A
// committed outcome is authoritative; a started outcome is replayed through
// the idempotent copy-only callback and read back by it.
func (c *Coordinator) runDBPhase(ctx context.Context, plan *Plan, repair DBRepairFunc) error {
	if repair == nil {
		return errors.New("maintenance: DB repair callback is required")
	}
	outcome, _, err := ReadDBOutcome(ctx, c.kv)
	if err != nil {
		return err
	}
	if outcome != nil {
		if outcome.RunID != plan.RunID {
			if outcome.State != DBStateCommitted {
				return errors.New("maintenance: an unfinished DB outcome belongs to a different run")
			}
			// A completed previous run's readback is not authority for this
			// request; replace it with this run's started/committed outcome.
		} else if outcome.PlanHash != plan.Hash() {
			return errors.New("maintenance: DB outcome belongs to a different plan")
		} else if outcome.State == DBStateCommitted {
			if err := Advance(ctx, c.kv, plan.RunID, PhaseDBRepaired, map[string]string{"content_hash": outcome.ContentHash}, "DB commit read back from durable outcome", c.now()); err != nil {
				return err
			}
			return nil
		}
	}
	started := &DBOutcome{Version: ControlVersion, RunID: plan.RunID, PlanHash: plan.Hash(), Database: plan.Database, AuditAsOf: plan.AuditAsOf, State: DBStateStarted}
	if err := PutDBOutcome(ctx, c.kv, started); err != nil {
		return err
	}
	result, err := repair(ctx, plan)
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("maintenance: DB repair returned no readback")
	}
	if !strings.EqualFold(result.Database, plan.Database) {
		return fmt.Errorf("maintenance: DB readback database %q does not match plan %q", result.Database, plan.Database)
	}
	if result.ContentHash != HashDBRows(result.Database, result.Rows) {
		return errors.New("maintenance: DB readback content hash mismatch")
	}
	committed := &DBOutcome{
		Version:     ControlVersion,
		RunID:       plan.RunID,
		PlanHash:    plan.Hash(),
		Database:    result.Database,
		AuditAsOf:   plan.AuditAsOf,
		State:       DBStateCommitted,
		CommittedAt: result.CommittedAt,
		Rows:        append([]DBRowOutcome(nil), result.Rows...),
		ContentHash: result.ContentHash,
	}
	if err := PutDBOutcome(ctx, c.kv, committed); err != nil {
		return err
	}
	return Advance(ctx, c.kv, plan.RunID, PhaseDBRepaired, map[string]string{"content_hash": committed.ContentHash}, "DB copy repair committed and read back", c.now())
}

// requireJournalPhases makes admission depend on every cross-store phase.
func (c *Coordinator) requireJournalPhases(ctx context.Context, runID string, phases ...string) error {
	journal, _, ok, err := ReadJournal(ctx, c.kv)
	if err != nil {
		return err
	}
	if !ok || journal.RunID != runID {
		return errors.New("maintenance: journal does not belong to this run")
	}
	for _, phase := range phases {
		if !journal.Has(phase) {
			return fmt.Errorf("maintenance: admission blocked; missing phase %s", phase)
		}
	}
	return nil
}

// AdmitFleet re-admits only floor-aware gateways after the corrected snapshot
// and baseline are durable.
func (c *Coordinator) AdmitFleet(ctx context.Context, runID string, expectedConsumers []string) error {
	if err := c.requireJournalPhases(ctx, runID, PhaseDBRepaired, PhaseHotReconciled, PhasePolicyPersisted, PhaseForwardCommitted, PhaseSnapshot, PhaseFloorActivated, PhaseDeliveryFenced); err != nil {
		return err
	}
	floor, _, err := ReadFloor(ctx, c.kv)
	if err != nil {
		return err
	}
	if !floor.Valid() {
		return errors.New("maintenance: cannot admit a fleet without a valid floor")
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if control.RunID != "" && control.RunID != runID {
		return fmt.Errorf("maintenance: control belongs to run %s", control.RunID)
	}
	if len(control.DeliveryConsumers) > 0 && !sameStringSet(control.DeliveryConsumers, expectedConsumers) {
		return errors.New("maintenance: consumer inventory changed during the fenced run")
	}
	if err := c.ResumeConsumers(ctx, expectedConsumers); err != nil {
		return err
	}
	control.DeliveryFenced = false
	control.DeliveryConsumers = nil
	control.FenceActive = false
	control.Required = true
	control.IngressClosed = false
	control.DownstreamFenced = false
	control.WriterClosed = false
	control.Phase = PhaseServing
	control.FleetEpoch++
	control.UpdatedAt = c.now().UnixNano()
	control.LastError = ""
	if err := Advance(ctx, c.kv, runID, PhaseServing, map[string]string{"fleet_epoch": fmt.Sprint(control.FleetEpoch)}, "fleet re-admitted", c.now()); err != nil {
		return err
	}
	return UpdateControl(ctx, c.kv, control)
}

// RecordFailure leaves the fence closed and records the phase error for the
// operator/resumer.
func (c *Coordinator) RecordFailure(ctx context.Context, cause error) {
	if cause == nil {
		return
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return
	}
	control.LastError = cause.Error()
	control.UpdatedAt = c.now().UnixNano()
	_ = UpdateControl(ctx, c.kv, control)
}

// DBRepairFunc performs the copy-only DB transaction owned by the repair
// script. It is mandatory for admission.
type DBRepairFunc func(context.Context, *Plan) (*DBRepairResult, error)

// ApplyRequest is one maintenance run. Apply performs no live DB/NATS action by
// itself: the caller supplies the already-authorized DB callback and the writer
// barrier source.
type ApplyRequest struct {
	Database         string
	EnvironmentID    string
	AsOf             time.Time
	Audit            []AuditArticle
	ConfirmOriginals bool
	DBRepair         DBRepairFunc
	AuditFunc        func(context.Context) (time.Time, []AuditArticle, error)
	// VerifyEnvironment re-reads the caller-owned database identity (and any
	// other durable target) before the first fence. It is mandatory so a
	// coordinator can never fence a bus for a database it did not verify.
	VerifyEnvironment func(context.Context) error
	Drain             DrainOptions
	ExpectedProducers []string
	ExcludedProducers []string
	ExpectedGateways  []string
	ExcludedGateways  []string
	ExpectedConsumers []string
	Exclusion         ExclusionProof
	DrainTimeout      time.Duration
}

// ApplyResult is the durable outcome of one run.
type ApplyResult struct {
	PlanRunID   string         `json:"plan_run_id"`
	TargetSeq   uint64         `json:"target_seq"`
	RawSeq      uint64         `json:"raw_seq"`
	SnapshotSeq uint64         `json:"snapshot_seq"`
	PolicyHash  string         `json:"policy_hash"`
	Floor       Floor          `json:"floor"`
	Forward     *ForwardReport `json:"forward"`
	HotChanged  int            `json:"hot_changed"`
	HotSkipped  int            `json:"hot_skipped"`
	AlreadyDone bool           `json:"already_done"`
}

// Apply executes the forward-only maintenance sequence. It is crash-resumable:
// completed journal phases are skipped and every remaining phase is idempotent.
func (c *Coordinator) Apply(ctx context.Context, req ApplyRequest) (*ApplyResult, error) {
	result := &ApplyResult{}
	if strings.EqualFold(strings.TrimSpace(req.Database), "dor") {
		return result, errors.New("maintenance: refusing the live dor database")
	}
	if err := c.validateEnvironment(ctx, req.EnvironmentID); err != nil {
		return result, err
	}
	if req.VerifyEnvironment == nil {
		return result, errors.New("maintenance: environment verification callback is required")
	}
	if err := req.VerifyEnvironment(ctx); err != nil {
		return result, err
	}
	requestID := RequestIdentity(req.Database, req.AsOf, req.Audit, req.ConfirmOriginals)
	resumePlan := false
	if req.AuditFunc == nil {
		if journal, _, ok, err := ReadJournal(ctx, c.kv); err == nil && ok && journal.Has(PhaseServing) {
			if gate, err := ReadGate(ctx, c.kv); err == nil && gate.Serving() && gate.Floor.Valid() && gate.Control.RunID == journal.RunID && gate.Floor.RequestID == requestID {
				result.PlanRunID = journal.RunID
				result.TargetSeq = gate.Floor.MinimumResumeSeq
				result.RawSeq = gate.Floor.BootstrapStreamSeq
				result.PolicyHash = gate.Floor.PolicyHash
				result.Floor = gate.Floor
				result.AlreadyDone = true
				return result, nil
			}
		}
	}
	if err := c.BeginGate(ctx, "begin timestamp maintenance"); err != nil {
		return result, err
	}
	// A fenced run with a persisted plan is a crash resume: its deterministic
	// request identity is authoritative even if the retry performed a fresh DB
	// audit with a later statement timestamp.
	if persisted, ok := c.activePlanRequestID(ctx); ok {
		requestID = persisted
		resumePlan = true
	}
	if err := ValidateExclusionProof(req.Exclusion); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if !sameStringSet(req.Exclusion.Expected, req.ExpectedGateways) || !sameStringSet(req.Exclusion.Excluded, req.ExcludedGateways) ||
		!sameStringSet(req.Exclusion.ExpectedProducers, req.ExpectedProducers) || !sameStringSet(req.Exclusion.ExcludedProducers, req.ExcludedProducers) {
		err := errors.New("maintenance: exclusion proof does not match expected/excluded participant inventory")
		c.RecordFailure(ctx, err)
		return result, err
	}
	if len(req.Exclusion.Excluded) > 0 || len(req.Exclusion.ExcludedProducers) > 0 {
		if c.authority == nil {
			err := errors.New("maintenance: excluded participants require a wired machine-verifying exclusion authority")
			c.RecordFailure(ctx, err)
			return result, err
		}
		if err := c.authority.Verify(ctx, req.Exclusion); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
	}
	timeout := req.DrainTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	if err := c.AwaitFleetExclusion(ctx, req.Exclusion, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if _, err := c.AwaitProducerAdmission(ctx, req.ExpectedProducers, req.ExcludedProducers, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.Drain(ctx, &req, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.PauseConsumers(ctx, req.ExpectedConsumers); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.RecheckAfterDeliveryFence(ctx, &req, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.CloseDownstream(ctx); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if _, err := c.AwaitProducerFence(ctx, req.ExpectedProducers, req.ExcludedProducers, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.CloseWriterAdmission(ctx); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if _, err := c.AwaitProducerWriterClosed(ctx, req.ExpectedProducers, req.ExcludedProducers, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if req.AuditFunc != nil && !resumePlan {
		asOf, audit, err := req.AuditFunc(ctx)
		if err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
		req.AsOf, req.Audit = asOf, audit
		requestID = RequestIdentity(req.Database, req.AsOf, req.Audit, req.ConfirmOriginals)
	}
	if c.store == nil {
		if err := c.ClaimPublisher(ctx); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
	}
	if _, err := c.SettleOutbox(ctx); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	plan, journal, err := c.loadOrPlan(ctx, &req)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.PlanRunID = plan.RunID
	if c.store == nil {
		if err := c.ClaimPublisher(ctx); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
	}
	if err := c.FenceForRun(ctx, plan.RunID); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if journal == nil || !journal.Has(PhasePlanned) {
		if err := PutPlan(ctx, c.kv, plan); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
		if err := Advance(ctx, c.kv, plan.RunID, PhasePlanned, map[string]string{"plan_hash": plan.Hash(), "audit_hash": plan.AuditHash}, "deterministic plan persisted", c.now()); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
	}
	if plan.RequestID != requestID {
		c.RecordFailure(ctx, errors.New("maintenance: plan request identity mismatch"))
		return result, errors.New("maintenance: plan request identity mismatch")
	}
	if err := c.runDBPhase(ctx, plan, req.DBRepair); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	policy, err := c.PersistPolicy(ctx, plan)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.PolicyHash = plan.PolicyHash
	hotChanged, hotSkipped, err := c.RepairHotArticles(ctx, plan)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.HotChanged, result.HotSkipped = hotChanged, hotSkipped
	forward, err := c.RepairRetained(ctx, plan, policy)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.Forward = forward
	result.TargetSeq, result.RawSeq = forward.TargetSeq, forward.RawSeq
	snapSeq, err := c.WriteSnapshot(ctx, plan)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.SnapshotSeq = snapSeq
	floor, err := c.ActivateFloor(ctx, plan, plan.PolicyHash)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.Floor = floor
	if err := c.AwaitConsumerQuiescence(ctx, req.ExpectedConsumers, timeout); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.AdmitFleet(ctx, plan.RunID, req.ExpectedConsumers); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if c.store != nil {
		_ = c.Release(ctx)
	}
	return result, nil
}

func (c *Coordinator) Drain(ctx context.Context, req *ApplyRequest, timeout time.Duration) error {
	journal, _, ok, err := ReadJournal(ctx, c.kv)
	if err != nil {
		return err
	}
	if ok && journal.Has(PhaseFenced) {
		return nil
	}
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil {
		return err
	}
	if control.DownstreamShut() {
		return nil
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	var previous *DrainReport
	for {
		producers, perr := c.AwaitProducerAdmission(ctx, req.ExpectedProducers, req.ExcludedProducers, 10*time.Second)
		if perr != nil {
			return perr
		}
		opts := req.Drain
		opts.Previous = previous
		opts.Producers = producers
		excluded := map[string]bool{}
		for _, id := range req.ExcludedProducers {
			excluded[id] = true
		}
		expected := make([]string, 0, len(req.ExpectedProducers))
		for _, id := range req.ExpectedProducers {
			if !excluded[id] {
				expected = append(expected, id)
			}
		}
		opts.ExpectedProducers = expected
		opts.ExpectedConsumers = append([]string(nil), req.ExpectedConsumers...)
		report, qerr := VerifyQuiesce(ctx, c.bus, opts)
		if qerr != nil {
			return qerr
		}
		if report.Stable {
			return Advance(ctx, c.kv, "", PhaseDraining, map[string]string{
				"delta_tail":  fmt.Sprint(report.DeltaTail),
				"ingest_last": fmt.Sprint(report.Streams[bus.StreamIngest].Last),
				"producers":   fmt.Sprint(len(producers)),
			}, "finite backlog drained", c.now())
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("maintenance: drain did not settle: %v", report.Reasons)
		}
		previous = report
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	am := map[string]int{}
	for _, v := range a {
		am[v]++
	}
	for _, v := range b {
		am[v]--
	}
	for _, n := range am {
		if n != 0 {
			return false
		}
	}
	return true
}

// activePlanRequestID returns the persisted plan's request identity when a
// fenced run is resuming. A blocked control with a planned journal is never
// re-planned from a later audit.
func (c *Coordinator) activePlanRequestID(ctx context.Context) (string, bool) {
	control, _, err := ReadControl(ctx, c.kv)
	if err != nil || !control.Blocked() || control.RunID == "" {
		return "", false
	}
	journal, _, ok, err := ReadJournal(ctx, c.kv)
	if err != nil || !ok || !journal.Has(PhasePlanned) {
		return "", false
	}
	plan, err := LoadPlan(ctx, c.kv)
	if err != nil || plan.RunID != control.RunID {
		return "", false
	}
	return plan.RequestID, true
}

func (c *Coordinator) loadOrPlan(ctx context.Context, req *ApplyRequest) (*Plan, *Journal, error) {
	if journal, _, ok, err := ReadJournal(ctx, c.kv); err == nil && ok && journal.Has(PhasePlanned) {
		plan, err := LoadPlan(ctx, c.kv)
		if err != nil {
			return nil, nil, err
		}
		return plan, journal, nil
	}
	plan, err := BuildPlan(ctx, c.bus, req.Database, req.AsOf, req.Audit, req.ConfirmOriginals)
	if err != nil {
		return nil, nil, err
	}
	return plan, nil, nil
}

// BranchPlan is retained for callers that want a read-only deterministic plan
// without acquiring the fence.
func BranchPlan(ctx context.Context, b *bus.Bus, req ApplyRequest) (*Plan, error) {
	return BuildPlan(ctx, b, req.Database, req.AsOf, req.Audit, req.ConfirmOriginals)
}

// SortedPolicyIDs returns policy article ids in a stable order for reports.
func SortedPolicyIDs(policy *WireTimePolicy) []string {
	if policy == nil {
		return nil
	}
	ids := make([]string, 0, len(policy.Entries))
	for id := range policy.Entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
