// Package maintenance implements the durable, cross-component protocol for
// repairing retained publication state without resetting source streams or
// rewriting already-published DELTA history.
//
// The package owns four kinds of non-expiring records in the publisher KV
// bucket:
//
//   - maintenance/control: the fleet fence and the current journaled phase;
//   - maintenance/floor: the monotonic gateway (global T, raw D) baseline;
//   - maintenance/wire_time_policy: durable per-article time overrides and
//     audited presentation suppression applied to retained, redelivered and
//     future input;
//   - maintenance/journal and maintenance/plan: the crash-resumable phase
//     history and the deterministic, revision-bound repair plan.
//
// Publishers and gateways read the policy/floor; the maintenance adapter is the
// only writer. A missing or corrupt active control record fails closed rather
// than falling back to a zero baseline.
package maintenance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/publication"
)

// Keys in the non-expiring publisher bucket. They never participate in the
// publication root/chunk lifecycle and therefore survive checkpoint rewrites
// and compaction.
const (
	KeyControl   = "maintenance/control"
	KeyFloor     = "maintenance/floor"
	KeyPolicy    = "maintenance/wire_time_policy"
	KeyJournal   = "maintenance/journal"
	KeyPlan      = "maintenance/plan"
	KeyDBOutcome = "maintenance/db_outcome"
	// KeyActivated is the non-expiring proof that a baseline was once
	// activated. It keeps a deleted control/floor pair from looking like a
	// fresh, floor-less deployment.
	KeyActivated = "maintenance/activated"

	// ControlVersion is the current control/floor/journal schema.
	ControlVersion = 1

	// chunkSize bounds one maintenance record chunk. It leaves room for the KV
	// subject and headers under the default NATS payload limit.
	chunkSize = 220 * 1024
	// recordLimit caps a decoded maintenance record.
	recordLimit = 64 * 1024 * 1024
)

// ErrFenced reports that the control record forbids the requested serving or
// writer action.
var ErrFenced = errors.New("maintenance: fleet fence active")

// ErrFloorRegression reports an attempt to lower the gateway baseline.
var ErrFloorRegression = errors.New("maintenance: floor regression")

// Journal phases, in forward-only order.
const (
	PhaseIdle             = "idle"
	PhaseDraining         = "draining"
	PhaseFenced           = "fenced"
	PhasePlanned          = "planned"
	PhaseDBRepaired       = "db_repaired"
	PhaseHotReconciled    = "hot_reconciled"
	PhasePolicyPersisted  = "policy_persisted"
	PhaseForwardCommitted = "forward_committed"
	PhaseSnapshot         = "snapshot_committed"
	PhaseFloorActivated   = "floor_activated"
	PhaseServing          = "serving"
)

var phaseRank = map[string]int{
	"":                    -1,
	PhaseIdle:             0,
	PhaseDraining:         1,
	PhaseFenced:           2,
	PhasePlanned:          3,
	PhaseDBRepaired:       4,
	PhasePolicyPersisted:  5,
	PhaseHotReconciled:    6,
	PhaseForwardCommitted: 7,
	PhaseSnapshot:         8,
	PhaseFloorActivated:   9,
	PhaseServing:          10,
}

// Control is the durable fleet fence and phase record. FenceActive is the
// serving exclusion; Phase journaling is diagnostic/resumable progress. A
// nonempty Phase other than idle/serving blocks new gateway admission.
type Control struct {
	Version     int    `json:"version"`
	Phase       string `json:"phase"`
	Required    bool   `json:"required"`
	FenceActive bool   `json:"fence_active"`
	RunID       string `json:"run_id,omitempty"`
	PlanHash    string `json:"plan_hash,omitempty"`
	FleetEpoch  uint64 `json:"fleet_epoch"`
	GlobalSeq   uint64 `json:"global_seq"`
	RawSeq      uint64 `json:"raw_seq"`
	UpdatedAt   int64  `json:"updated_at"`
	LastError   string `json:"last_error,omitempty"`
	// ExclusionEvidence is the operator's explicit proof that any expected
	// participant not present in-band is excluded from routing/processes.
	ExclusionEvidence string `json:"exclusion_evidence,omitempty"`
}

// Blocked reports whether the control record forbids serving.
func (c Control) Blocked() bool {
	if c.FenceActive {
		return true
	}
	switch c.Phase {
	case "", PhaseIdle, PhaseServing:
		return false
	default:
		return true
	}
}

// FloorRequired reports whether a valid floor record is mandatory. Once any
// maintenance run activates a baseline, a later missing/corrupt control or
// floor record must keep gateways unready.
func (c Control) FloorRequired() bool {
	return c.Required || phaseRank[c.Phase] >= phaseRank[PhaseFloorActivated]
}

// Floor is the monotonic gateway boot/resume baseline. GlobalSeq is the last
// global DELTA sequence included in the corrected snapshot; BootstrapStreamSeq
// is the raw JetStream sequence of the batch that ended at GlobalSeq.
type Floor struct {
	Version            int    `json:"version"`
	Initialized        bool   `json:"initialized"`
	MinimumResumeSeq   uint64 `json:"minimum_resume_seq"`
	BootstrapStreamSeq uint64 `json:"bootstrap_stream_seq"`
	RepairID           string `json:"repair_id,omitempty"`
	RunID              string `json:"run_id,omitempty"`
	PolicyHash         string `json:"policy_hash,omitempty"`
	RequestID          string `json:"request_id,omitempty"`
	WireTombstone      bool   `json:"wire_tombstone"`
	ActivatedAt        int64  `json:"activated_at"`
}

// Valid reports whether the floor can gate a gateway. A zero-initialized floor
// is only the pre-maintenance bootstrap state and never satisfies an active
// control record.
func (f Floor) Valid() bool {
	return f.Version == ControlVersion && f.Initialized && f.MinimumResumeSeq > 0 && f.WireTombstone
}

// Gate is the consistent pair read by a gateway. Initialized distinguishes a
// legacy/bootstrap deployment (no records) from an active maintenance run.
type Gate struct {
	Control         Control
	ControlRevision uint64
	Floor           Floor
	FloorRevision   uint64
	Initialized     bool
}

// Serving reports whether this gateway may accept clients and deltas.
func (g Gate) Serving() bool { return !g.Control.Blocked() }

// ReadControl reads the control record. A missing record is reported as a zero
// value and revision zero; a corrupt record is an error.
func ReadControl(ctx context.Context, kv jetstream.KeyValue) (Control, uint64, error) {
	entry, err := kv.Get(ctx, KeyControl)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Control{}, 0, nil
	}
	if err != nil {
		return Control{}, 0, fmt.Errorf("maintenance: read control: %w", err)
	}
	var c Control
	if err := json.Unmarshal(entry.Value(), &c); err != nil {
		return Control{}, 0, fmt.Errorf("maintenance: corrupt control record: %w", err)
	}
	if c.Version != ControlVersion {
		return Control{}, 0, fmt.Errorf("maintenance: unsupported control version %d", c.Version)
	}
	return c, entry.Revision(), nil
}

// ReadFloor reads the floor record. A missing record is reported as a zero
// value and revision zero.
func ReadFloor(ctx context.Context, kv jetstream.KeyValue) (Floor, uint64, error) {
	entry, err := kv.Get(ctx, KeyFloor)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Floor{}, 0, nil
	}
	if err != nil {
		return Floor{}, 0, fmt.Errorf("maintenance: read floor: %w", err)
	}
	var f Floor
	if err := json.Unmarshal(entry.Value(), &f); err != nil {
		return Floor{}, 0, fmt.Errorf("maintenance: corrupt floor record: %w", err)
	}
	if f.Version != ControlVersion {
		return Floor{}, 0, fmt.Errorf("maintenance: unsupported floor version %d", f.Version)
	}
	return f, entry.Revision(), nil
}

// Activation is the durable marker written with the first floor.
type Activation struct {
	Version   int    `json:"version"`
	RunID     string `json:"run_id"`
	GlobalSeq uint64 `json:"global_seq"`
	RawSeq    uint64 `json:"raw_seq"`
	Activated int64  `json:"activated_at"`
}

// ReadActivation reads the activation marker. Missing is (nil, 0, nil).
func ReadActivation(ctx context.Context, kv jetstream.KeyValue) (*Activation, uint64, error) {
	entry, err := kv.Get(ctx, KeyActivated)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var a Activation
	if err := json.Unmarshal(entry.Value(), &a); err != nil {
		return nil, 0, fmt.Errorf("maintenance: corrupt activation marker: %w", err)
	}
	if a.Version != ControlVersion {
		return nil, 0, errors.New("maintenance: unsupported activation version")
	}
	return &a, entry.Revision(), nil
}

// ReadGate enforces the fail-closed relationship between control and floor.
// Both missing means an uninitialized deployment; an active floor without its
// control, a corrupt record or a required-but-invalid floor is an error.
func ReadGate(ctx context.Context, kv jetstream.KeyValue) (Gate, error) {
	control, crev, err := ReadControl(ctx, kv)
	if err != nil {
		return Gate{}, err
	}
	floor, frev, err := ReadFloor(ctx, kv)
	if err != nil {
		return Gate{}, err
	}
	activation, _, err := ReadActivation(ctx, kv)
	if err != nil {
		return Gate{}, err
	}
	g := Gate{Control: control, ControlRevision: crev, Floor: floor, FloorRevision: frev}
	if crev == 0 && frev == 0 && activation == nil {
		return g, nil
	}
	if activation != nil && (crev == 0 || !floor.Valid()) {
		return Gate{}, errors.New("maintenance: activation marker exists but control/floor is missing or invalid")
	}
	if crev == 0 && floor.Initialized {
		return Gate{}, errors.New("maintenance: active floor without control record")
	}
	if control.FloorRequired() && !floor.Valid() {
		return Gate{}, errors.New("maintenance: active maintenance control requires a valid floor")
	}
	g.Initialized = true
	return g, nil
}

// WireTimePolicy is the durable per-article timestamp authority. It is applied
// by the publisher to restored state, every future/redelivered article and the
// wire row, so a stale replay cannot reintroduce a repaired time.
type WireTimePolicy struct {
	Version     int                      `json:"version"`
	RunID       string                   `json:"run_id"`
	PlanHash    string                   `json:"plan_hash"`
	GeneratedAt int64                    `json:"generated_at"`
	Entries     map[string]WireTimeEntry `json:"entries"`
}

// WireTimeEntry is one article's audited disposition.
type WireTimeEntry struct {
	ArticleID           string `json:"article_id"`
	Mode                string `json:"mode"` // override or suppress
	ObservedPublishedAt int64  `json:"observed_published_at"`
	CreatedAt           int64  `json:"created_at"`
	OverrideAt          int64  `json:"override_at,omitempty"`
	Reason              string `json:"reason"`
	AuditedAt           int64  `json:"audited_at"`
	RawEvidenceHash     string `json:"raw_evidence_hash,omitempty"`
}

const (
	ModeOverride = "override"
	ModeSuppress = "suppress"
)

// Hash returns the deterministic policy identity. It binds the article
// dispositions only: the run id, plan hash and generation time are audit
// metadata and are deliberately excluded, so the policy written by the forward
// repair has the same identity the plan recorded.
func (p *WireTimePolicy) Hash() string {
	if p == nil {
		return ""
	}
	clone := *p
	clone.RunID = ""
	clone.PlanHash = ""
	clone.GeneratedAt = 0
	copy := make(map[string]WireTimeEntry, len(p.Entries))
	for id, e := range p.Entries {
		copy[id] = e
	}
	clone.Entries = copy
	data, _ := json.Marshal(clone)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Validate checks structural invariants without needing publisher state.
func (p *WireTimePolicy) Validate() error {
	if p == nil {
		return errors.New("maintenance: nil policy")
	}
	if p.Version != ControlVersion {
		return fmt.Errorf("maintenance: unsupported policy version %d", p.Version)
	}
	for id, e := range p.Entries {
		if id == "" || e.ArticleID != id {
			return errors.New("maintenance: policy article identity mismatch")
		}
		switch e.Mode {
		case ModeOverride:
			if e.OverrideAt <= 0 || e.OverrideAt == e.ObservedPublishedAt {
				return fmt.Errorf("maintenance: invalid override for %s", id)
			}
		case ModeSuppress:
			if e.OverrideAt != 0 {
				return fmt.Errorf("maintenance: suppression carries an override for %s", id)
			}
		default:
			return fmt.Errorf("maintenance: unknown policy mode %q for %s", e.Mode, id)
		}
	}
	return nil
}

// EntryFor returns the policy entry for a canonical article id, if any.
func (p *WireTimePolicy) EntryFor(articleID string) (WireTimeEntry, bool) {
	if p == nil {
		return WireTimeEntry{}, false
	}
	e, ok := p.Entries[articleID]
	return e, ok
}

// ApplyPolicy returns the timestamp a wire row must carry. Suppression is
// represented by a tombstone timestamp because DELTA has no dedicated remove
// event; publication/snapshot/gateway/web all treat a negative At as hidden.
func ApplyPolicy(p *WireTimePolicy, articleID string, at int64) int64 {
	if e, ok := p.EntryFor(articleID); ok {
		switch e.Mode {
		case ModeOverride:
			return e.OverrideAt
		case ModeSuppress:
			return publication.WireSuppressedAt
		}
	}
	return at
}

// Journal records forward-only phases. Values are strings so JSON round trips
// cannot silently lose uint64 precision.
type Journal struct {
	Version int           `json:"version"`
	RunID   string        `json:"run_id"`
	Phases  []PhaseRecord `json:"phases"`
}

// PhaseRecord is one journaled transition.
type PhaseRecord struct {
	Phase    string            `json:"phase"`
	At       int64             `json:"at"`
	PlanHash string            `json:"plan_hash,omitempty"`
	Values   map[string]string `json:"values,omitempty"`
	Note     string            `json:"note,omitempty"`
}

// Last returns the last journaled phase.
func (j *Journal) Last() string {
	if j == nil || len(j.Phases) == 0 {
		return ""
	}
	return j.Phases[len(j.Phases)-1].Phase
}

// Has reports whether a phase was recorded for this run.
func (j *Journal) Has(phase string) bool {
	for _, p := range j.Phases {
		if p.Phase == phase {
			return true
		}
	}
	return false
}

// Plan is the deterministic, revision-bound maintenance plan. RunID is the
// SHA-256 of the canonical plan with RunID and CreatedAt cleared, so identical
// evidence and revisions yield the same identity.
type Plan struct {
	Version          int                 `json:"version"`
	RunID            string              `json:"run_id"`
	RequestID        string              `json:"request_id"`
	CreatedAt        int64               `json:"created_at"`
	Database         string              `json:"database"`
	AuditAsOf        int64               `json:"audit_as_of"`
	ConfirmOriginals bool                `json:"confirm_originals"`
	BasePolicyHash   string              `json:"base_policy_hash,omitempty"`
	RootRevision     uint64              `json:"root_revision"`
	RootOrdinal      uint64              `json:"root_ordinal"`
	RootSeq          uint64              `json:"root_seq"`
	RootRaw          uint64              `json:"root_raw"`
	SnapshotRevision uint64              `json:"snapshot_revision"`
	IngestFirst      uint64              `json:"ingest_first"`
	IngestLast       uint64              `json:"ingest_last"`
	DeltaTail        uint64              `json:"delta_tail"`
	Articles         []PlanArticle       `json:"articles"`
	HotArticles      []HotArticleBinding `json:"hot_articles"`
	PolicyHash       string              `json:"policy_hash"`
	AuditHash        string              `json:"audit_hash"`
}

// HotArticleBinding binds the live_state article key that carries the clean
// Article.PublishedAt used by matcher backfill and story details.
type HotArticleBinding struct {
	ArticleID  string `json:"article_id"`
	Key        string `json:"key"`
	Present    bool   `json:"present"`
	Revision   uint64 `json:"revision,omitempty"`
	BeforeHash string `json:"before_hash"`
	TargetAt   int64  `json:"target_at"`
	Mode       string `json:"mode"`
}

// PlanArticle binds one audited article to the state observed under the fence.
type PlanArticle struct {
	ArticleID           string `json:"article_id"`
	StoryID             string `json:"story_id,omitempty"`
	Mode                string `json:"mode"`
	ObservedPublishedAt int64  `json:"observed_published_at"`
	CreatedAt           int64  `json:"created_at"`
	OverrideAt          int64  `json:"override_at,omitempty"`
	RawEvidenceHash     string `json:"raw_evidence_hash,omitempty"`
	Reason              string `json:"reason"`
	Carried             bool   `json:"carried,omitempty"`
	Present             bool   `json:"present"`
	BeforeHash          string `json:"before_hash"`
}

// Hash returns the canonical run identity.
func (p *Plan) Hash() string {
	if p == nil {
		return ""
	}
	clone := *p
	clone.RunID = ""
	clone.CreatedAt = 0
	clone.PolicyHash = ""
	clone.Articles = append([]PlanArticle(nil), p.Articles...)
	sort.Slice(clone.Articles, func(i, j int) bool { return clone.Articles[i].ArticleID < clone.Articles[j].ArticleID })
	clone.HotArticles = append([]HotArticleBinding(nil), p.HotArticles...)
	sort.Slice(clone.HotArticles, func(i, j int) bool { return clone.HotArticles[i].ArticleID < clone.HotArticles[j].ArticleID })
	data, _ := json.Marshal(clone)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Verify checks structural invariants and recomputes the run identity.
func (p *Plan) Verify() error {
	if p == nil {
		return errors.New("maintenance: nil plan")
	}
	if p.Version != ControlVersion {
		return fmt.Errorf("maintenance: unsupported plan version %d", p.Version)
	}
	if p.RunID == "" || p.RunID != p.Hash() {
		return errors.New("maintenance: plan run identity mismatch")
	}
	if p.RootRevision == 0 && p.RootOrdinal == 0 && p.RootSeq == 0 {
		return errors.New("maintenance: plan has no publisher root binding")
	}
	if p.PolicyHash != "" && p.PolicyHash != p.Policy().Hash() {
		return errors.New("maintenance: plan policy hash mismatch")
	}
	seen := map[string]bool{}
	for _, a := range p.Articles {
		if a.ArticleID == "" || seen[a.ArticleID] {
			return errors.New("maintenance: duplicate or empty plan article")
		}
		seen[a.ArticleID] = true
		switch a.Mode {
		case ModeOverride:
			if a.OverrideAt <= 0 || a.OverrideAt == a.ObservedPublishedAt {
				return fmt.Errorf("maintenance: invalid override for %s", a.ArticleID)
			}
		case ModeSuppress:
			if a.OverrideAt != 0 {
				return fmt.Errorf("maintenance: suppression carries an override for %s", a.ArticleID)
			}
		default:
			return fmt.Errorf("maintenance: unknown plan mode %q", a.Mode)
		}
	}
	hot := map[string]bool{}
	for _, h := range p.HotArticles {
		if h.ArticleID == "" || hot[h.ArticleID] {
			return errors.New("maintenance: duplicate or empty hot article binding")
		}
		if h.Mode != ModeOverride && h.Mode != ModeSuppress {
			return fmt.Errorf("maintenance: unknown hot article mode %q", h.Mode)
		}
		hot[h.ArticleID] = true
	}
	return nil
}

// DBRowOutcome is the durable readback for one audited DB article.
type DBRowOutcome struct {
	ArticleID       string `json:"article_id"`
	BeforePublished int64  `json:"before_published"`
	BeforeCreated   int64  `json:"before_created"`
	AfterPublished  int64  `json:"after_published"`
	Suppressed      bool   `json:"suppressed"`
}

// HashDBRows is the deterministic DB result identity.
func HashDBRows(database string, rows []DBRowOutcome) string {
	copy := append([]DBRowOutcome(nil), rows...)
	sort.Slice(copy, func(i, j int) bool { return copy[i].ArticleID < copy[j].ArticleID })
	data, _ := CanonicalJSON(struct {
		Database string         `json:"database"`
		Rows     []DBRowOutcome `json:"rows"`
	}{Database: database, Rows: copy})
	return hashBytes(data)
}

// DBRepairResult is the transaction result the operator adapter must return.
// ContentHash is SHA-256 over the sorted outcomes and the database name.
type DBRepairResult struct {
	Database    string         `json:"database"`
	CommittedAt int64          `json:"committed_at"`
	Rows        []DBRowOutcome `json:"rows"`
	ContentHash string         `json:"content_hash"`
}

// DBOutcome is the durable cross-store record that makes a DB commit and the
// maintenance journal crash-consistent.
type DBOutcome struct {
	Version     int            `json:"version"`
	RunID       string         `json:"run_id"`
	PlanHash    string         `json:"plan_hash"`
	Database    string         `json:"database"`
	AuditAsOf   int64          `json:"audit_as_of"`
	State       string         `json:"state"` // started or committed
	CommittedAt int64          `json:"committed_at,omitempty"`
	Rows        []DBRowOutcome `json:"rows,omitempty"`
	ContentHash string         `json:"content_hash,omitempty"`
}

const (
	DBStateStarted   = "started"
	DBStateCommitted = "committed"
)

// ReadDBOutcome loads the active DB outcome. Missing is (nil, 0, nil).
func ReadDBOutcome(ctx context.Context, kv jetstream.KeyValue) (*DBOutcome, uint64, error) {
	var out DBOutcome
	rev, ok, err := readRecord(ctx, kv, KeyDBOutcome, &out)
	if err != nil || !ok {
		return nil, 0, err
	}
	if out.Version != ControlVersion {
		return nil, 0, errors.New("maintenance: unsupported DB outcome version")
	}
	return &out, rev, nil
}

// PutDBOutcome CASes the DB outcome.
func PutDBOutcome(ctx context.Context, kv jetstream.KeyValue, out *DBOutcome) error {
	out.Version = ControlVersion
	var current DBOutcome
	rev, ok, err := readRecord(ctx, kv, KeyDBOutcome, &current)
	if err != nil {
		return err
	}
	if !ok {
		rev = 0
	}
	_, err = writeRecord(ctx, kv, KeyDBOutcome, out, rev)
	return err
}

// Policy derives the durable policy from a verified plan.
func (p *Plan) Policy() *WireTimePolicy {
	out := &WireTimePolicy{Version: ControlVersion, RunID: p.RunID, PlanHash: p.Hash(), GeneratedAt: p.CreatedAt, Entries: make(map[string]WireTimeEntry, len(p.Articles))}
	for _, a := range p.Articles {
		out.Entries[a.ArticleID] = WireTimeEntry{
			ArticleID:           a.ArticleID,
			Mode:                a.Mode,
			ObservedPublishedAt: a.ObservedPublishedAt,
			CreatedAt:           a.CreatedAt,
			OverrideAt:          a.OverrideAt,
			Reason:              a.Reason,
			AuditedAt:           p.AuditAsOf,
			RawEvidenceHash:     a.RawEvidenceHash,
		}
	}
	return out
}

// writeJSON CASes a JSON record, creating it when absent.
func writeJSON(ctx context.Context, kv jetstream.KeyValue, key string, value any, revision uint64) (uint64, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	if revision == 0 {
		return kv.Create(ctx, key, data)
	}
	return kv.Update(ctx, key, data, revision)
}

// writeRecord stores a value inline when it fits, otherwise as a hash-addressed
// chunk manifest. The manifest itself is small enough for one KV value.
func writeRecord(ctx context.Context, kv jetstream.KeyValue, key string, value any, revision uint64) (uint64, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	if err := checkRecordSize(len(data)); err != nil {
		return 0, err
	}
	limit := int64(kvPayloadLimit(kv))
	if int64(len(data))+1024 <= limit {
		if revision == 0 {
			return kv.Create(ctx, key, data)
		}
		return kv.Update(ctx, key, data, revision)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	manifest := recordManifest{Version: ControlVersion, Hash: hash, Size: len(data)}
	for offset := 0; offset < len(data); offset += chunkSize {
		end := min(offset+chunkSize, len(data))
		chunkKey := fmt.Sprintf("maintenance.chunk.%s.%d", hash, len(manifest.Chunks))
		if _, err := kv.Get(ctx, chunkKey); errors.Is(err, jetstream.ErrKeyNotFound) {
			if _, err = kv.Create(ctx, chunkKey, data[offset:end]); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
				return 0, err
			}
		} else if err != nil {
			return 0, err
		}
		manifest.Chunks = append(manifest.Chunks, chunkKey)
	}
	wire, err := json.Marshal(manifest)
	if err != nil {
		return 0, err
	}
	if revision == 0 {
		return kv.Create(ctx, key, wire)
	}
	return kv.Update(ctx, key, wire, revision)
}

type recordManifest struct {
	Version int      `json:"record_version"`
	Hash    string   `json:"hash"`
	Size    int      `json:"size"`
	Chunks  []string `json:"chunks"`
}

// readRecord decodes an inline or chunked record and verifies its hash.
func readRecord(ctx context.Context, kv jetstream.KeyValue, key string, value any) (uint64, bool, error) {
	entry, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	data := entry.Value()
	var manifest recordManifest
	if json.Unmarshal(data, &manifest) == nil && manifest.Version == ControlVersion && len(manifest.Chunks) > 0 {
		if manifest.Size <= 0 || manifest.Size > recordLimit || len(manifest.Hash) != 64 {
			return 0, false, errors.New("maintenance: invalid chunk manifest")
		}
		data = make([]byte, 0, manifest.Size)
		for i, chunkKey := range manifest.Chunks {
			if chunkKey != fmt.Sprintf("maintenance.chunk.%s.%d", manifest.Hash, i) {
				return 0, false, errors.New("maintenance: invalid chunk order")
			}
			chunk, err := kv.Get(ctx, chunkKey)
			if err != nil {
				return 0, false, err
			}
			data = append(data, chunk.Value()...)
		}
		if len(data) != manifest.Size || fmt.Sprintf("%x", sha256.Sum256(data)) != manifest.Hash {
			return 0, false, errors.New("maintenance: chunked record hash mismatch")
		}
	}
	if err := checkRecordSize(len(data)); err != nil {
		return 0, false, err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return 0, false, err
	}
	return entry.Revision(), true, nil
}

func checkRecordSize(n int) error {
	if n <= 0 || n > recordLimit {
		return errors.New("maintenance: record size out of bounds")
	}
	return nil
}

// kvPayloadLimit keeps maintenance records below the server's maximum payload.
// The KV store does not expose the connection limit, so a conservative value is
// used; the embedded server defaults to 1 MiB.
func kvPayloadLimit(jetstream.KeyValue) int { return 1 << 20 }

// LoadPolicy reads the durable wire-time policy. Missing policy is (nil, nil).
func LoadPolicy(ctx context.Context, kv jetstream.KeyValue) (*WireTimePolicy, error) {
	var p WireTimePolicy
	_, ok, err := readRecord(ctx, kv, KeyPolicy, &p)
	if err != nil || !ok {
		return nil, err
	}
	if err = p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// PutPolicy persists a validated policy and returns its hash.
func PutPolicy(ctx context.Context, kv jetstream.KeyValue, p *WireTimePolicy) (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	hash := p.Hash()
	var current WireTimePolicy
	revision, ok, err := readRecord(ctx, kv, KeyPolicy, &current)
	if err != nil {
		return "", err
	}
	if !ok {
		revision = 0
	}
	if _, err = writeRecord(ctx, kv, KeyPolicy, p, revision); err != nil {
		return "", err
	}
	return hash, nil
}

// ReadJournal loads the journal. Missing journal is (nil, 0, false, nil).
func ReadJournal(ctx context.Context, kv jetstream.KeyValue) (*Journal, uint64, bool, error) {
	var j Journal
	rev, ok, err := readRecord(ctx, kv, KeyJournal, &j)
	if err != nil || !ok {
		return nil, 0, ok, err
	}
	if j.Version != ControlVersion {
		return nil, 0, false, errors.New("maintenance: unsupported journal version")
	}
	return &j, rev, true, nil
}

// ResetJournal starts a fresh forward-only journal for a new maintenance run.
// The control record retains the previous plan hash and baseline for audit.
func ResetJournal(ctx context.Context, kv jetstream.KeyValue) error {
	var j Journal
	rev, ok, err := readRecord(ctx, kv, KeyJournal, &j)
	if err != nil {
		return err
	}
	if !ok {
		rev = 0
	}
	_, err = writeRecord(ctx, kv, KeyJournal, &Journal{Version: ControlVersion}, rev)
	return err
}

// Advance appends one forward-only phase. Repeating the same phase for the same
// run is idempotent; moving backwards is an error.
func Advance(ctx context.Context, kv jetstream.KeyValue, runID, phase string, values map[string]string, note string, now time.Time) error {
	if phaseRank[phase] < 0 {
		return fmt.Errorf("maintenance: unknown journal phase %q", phase)
	}
	for attempt := 0; attempt < 8; attempt++ {
		var j Journal
		rev, ok, err := readRecord(ctx, kv, KeyJournal, &j)
		if err != nil {
			return err
		}
		if !ok {
			j = Journal{Version: ControlVersion, RunID: runID}
			rev = 0
		}
		if j.RunID != "" && j.RunID != runID {
			return fmt.Errorf("maintenance: journal belongs to run %s", j.RunID)
		}
		if j.RunID == "" {
			j.RunID = runID
		}
		last := j.Last()
		if last == phase {
			return nil
		}
		if phaseRank[phase] < phaseRank[last] {
			return fmt.Errorf("maintenance: cannot move journal backwards from %s to %s", last, phase)
		}
		j.Phases = append(j.Phases, PhaseRecord{Phase: phase, At: now.UnixNano(), Values: values, Note: note})
		if _, err = writeRecord(ctx, kv, KeyJournal, &j, rev); err == nil {
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
	}
	return errors.New("maintenance: journal contention")
}

// PutPlan persists the active plan after verifying its run identity.
func PutPlan(ctx context.Context, kv jetstream.KeyValue, plan *Plan) error {
	if err := plan.Verify(); err != nil {
		return err
	}
	var current Plan
	rev, ok, err := readRecord(ctx, kv, KeyPlan, &current)
	if err != nil {
		return err
	}
	if !ok {
		rev = 0
	}
	_, err = writeRecord(ctx, kv, KeyPlan, plan, rev)
	return err
}

// LoadPlan reads the active plan.
func LoadPlan(ctx context.Context, kv jetstream.KeyValue) (*Plan, error) {
	var p Plan
	_, ok, err := readRecord(ctx, kv, KeyPlan, &p)
	if err != nil || !ok {
		return nil, err
	}
	if err = p.Verify(); err != nil {
		return nil, err
	}
	return &p, nil
}

// UpdateControl CAS-writes the control record.
func UpdateControl(ctx context.Context, kv jetstream.KeyValue, c Control) error {
	for attempt := 0; attempt < 8; attempt++ {
		current, rev, err := ReadControl(ctx, kv)
		if err != nil {
			return err
		}
		_ = current
		c.Version = ControlVersion
		if _, err = writeJSON(ctx, kv, KeyControl, c, rev); err == nil {
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
	}
	return errors.New("maintenance: control contention")
}

// UpdateFloor CAS-writes the floor record and rejects regression.
func UpdateFloor(ctx context.Context, kv jetstream.KeyValue, f Floor) error {
	for attempt := 0; attempt < 8; attempt++ {
		current, rev, err := ReadFloor(ctx, kv)
		if err != nil {
			return err
		}
		if current.Valid() && (f.MinimumResumeSeq < current.MinimumResumeSeq || f.BootstrapStreamSeq < current.BootstrapStreamSeq) {
			return ErrFloorRegression
		}
		f.Version = ControlVersion
		if _, err = writeJSON(ctx, kv, KeyFloor, f, rev); err == nil {
			return nil
		} else if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return err
		}
	}
	return errors.New("maintenance: floor contention")
}

// CanonicalJSON is used by plan/audit hashing.
func CanonicalJSON(value any) ([]byte, error) {
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(out.Bytes()), nil
}
