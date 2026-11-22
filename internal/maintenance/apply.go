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
	bus    *bus.Bus
	kv     jetstream.KeyValue
	leases jetstream.KeyValue
	store  *publication.Store
	now    func() time.Time
}

// NewCoordinator binds the non-expiring publisher bucket and the short-lived
// gateway lease bucket.
func NewCoordinator(ctx context.Context, b *bus.Bus) (*Coordinator, error) {
	if b == nil {
		return nil, errors.New("maintenance: bus is required")
	}
	kv, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return nil, err
	}
	leases, err := b.KV(ctx, bus.KVBucketGatewayLease)
	if err != nil {
		return nil, err
	}
	return &Coordinator{bus: b, kv: kv, leases: leases, now: time.Now}, nil
}

// BeginGate closes gateway ingress before any drain. It is idempotent and
// deliberately does not require a plan: the fleet must be excluded even if the
// process stops before planning.
func (c *Coordinator) BeginGate(ctx context.Context, reason string) error {
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
		if err := ResetJournal(ctx, c.kv); err != nil {
			return err
		}
	}
	control.FenceActive = true
	control.Required = true
	control.Phase = PhaseDraining
	control.UpdatedAt = c.now().UnixNano()
	if reason != "" {
		control.LastError = ""
	}
	return UpdateControl(ctx, c.kv, control)
}

// AwaitFleetExclusion waits for floor-aware gateways to acknowledge the fence.
func (c *Coordinator) AwaitFleetExclusion(ctx context.Context, timeout time.Duration) error {
	if c.leases == nil {
		return errors.New("maintenance: gateway lease bucket unavailable")
	}
	return AwaitGatewayExclusion(ctx, c.leases, timeout)
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
	return c.store.Release(ctx)
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

// AdmitFleet re-admits only floor-aware gateways after the corrected snapshot
// and baseline are durable.
func (c *Coordinator) AdmitFleet(ctx context.Context, runID string) error {
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
	control.FenceActive = false
	control.Required = true
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
// script. It may be nil, in which case the DB phase is skipped.
type DBRepairFunc func(context.Context, *Plan) error

// ApplyRequest is one maintenance run. Apply performs no live DB/NATS action by
// itself: the caller supplies the already-authorized DB callback and the writer
// barrier source.
type ApplyRequest struct {
	Database         string
	AsOf             time.Time
	Audit            []AuditArticle
	ConfirmOriginals bool
	DBRepair         DBRepairFunc
	Drain            DrainOptions
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
	AlreadyDone bool           `json:"already_done"`
}

// Apply executes the forward-only maintenance sequence. It is crash-resumable:
// completed journal phases are skipped and every remaining phase is idempotent.
func (c *Coordinator) Apply(ctx context.Context, req ApplyRequest) (*ApplyResult, error) {
	result := &ApplyResult{}
	if strings.EqualFold(strings.TrimSpace(req.Database), "dor") {
		return result, errors.New("maintenance: refusing the live dor database")
	}
	if journal, _, ok, err := ReadJournal(ctx, c.kv); err == nil && ok && journal.Has(PhaseServing) {
		if gate, err := ReadGate(ctx, c.kv); err == nil && gate.Serving() && gate.Floor.Valid() && gate.Control.RunID == journal.RunID {
			result.PlanRunID = journal.RunID
			result.TargetSeq = gate.Floor.MinimumResumeSeq
			result.RawSeq = gate.Floor.BootstrapStreamSeq
			result.PolicyHash = gate.Floor.PolicyHash
			result.Floor = gate.Floor
			result.AlreadyDone = true
			return result, nil
		}
	}
	if err := c.BeginGate(ctx, "begin timestamp maintenance"); err != nil {
		return result, err
	}
	if err := c.AwaitFleetExclusion(ctx, 10*time.Second); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	if err := c.resumeOrDrain(ctx, &req); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
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
	if req.DBRepair != nil && (journal == nil || !journal.Has(PhaseDBRepaired)) {
		if err := req.DBRepair(ctx, plan); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
		if err := Advance(ctx, c.kv, plan.RunID, PhaseDBRepaired, nil, "DB copy repair committed", c.now()); err != nil {
			c.RecordFailure(ctx, err)
			return result, err
		}
	}
	policy, err := c.PersistPolicy(ctx, plan)
	if err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	result.PolicyHash = plan.PolicyHash
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
	if err := c.AdmitFleet(ctx, plan.RunID); err != nil {
		c.RecordFailure(ctx, err)
		return result, err
	}
	return result, nil
}

func (c *Coordinator) resumeOrDrain(ctx context.Context, req *ApplyRequest) error {
	journal, _, ok, err := ReadJournal(ctx, c.kv)
	if err != nil {
		return err
	}
	if ok && journal.Has(PhaseFenced) {
		return nil
	}
	first, err := VerifyQuiesce(ctx, c.bus, req.Drain)
	if err != nil {
		return err
	}
	opts := req.Drain
	opts.Previous = first
	second, err := VerifyQuiesce(ctx, c.bus, opts)
	if err != nil {
		return err
	}
	if !second.Stable {
		return fmt.Errorf("maintenance: drain barrier is not stable: %v", second.Reasons)
	}
	return Advance(ctx, c.kv, "", PhaseDraining, map[string]string{"delta_tail": fmt.Sprint(second.DeltaTail)}, "finite backlog drained", c.now())
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
