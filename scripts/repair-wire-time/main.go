// Command repair-wire-time audits retained future publication times and prepares
// candidate fetch-time repairs against one explicitly bound isolated copy. It
// never starts or provisions NATS: --nats must name an already provisioned
// server, and --environment-id must match the durable database, publisher and
// participant identities before any audit or maintenance step.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/maintenance"
	dorv1 "github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type candidate struct {
	ID         string    `json:"id"`
	Published  time.Time `json:"published"`
	Created    time.Time `json:"created"`
	StoryID    string    `json:"story_id,omitempty"`
	FetchTimes []int64   `json:"raw_fetch_times"`
	RepairAt   int64     `json:"candidate_repair_at,omitempty"`
	Reason     string    `json:"reason"`
}

type report struct {
	Database         string                   `json:"database"`
	Environment      string                   `json:"environment"`
	EnvironmentBound bool                     `json:"environment_bound,omitempty"`
	AsOf             time.Time                `json:"as_of"`
	IngestFirst      uint64                   `json:"ingest_first"`
	IngestLast       uint64                   `json:"ingest_last"`
	Changes          []*candidate             `json:"changes"`
	Committed        bool                     `json:"committed"`
	Plan             *maintenance.Plan        `json:"maintenance_plan,omitempty"`
	Maintenance      *maintenance.ApplyResult `json:"maintenance_result,omitempty"`
	Warning          string                   `json:"warning"`
}

// stringList collects a repeatable string flag.
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	v = strings.TrimSpace(v)
	if v != "" {
		*s = append(*s, v)
	}
	return nil
}

func rawID(raw *dorv1.RawArticle) string {
	u, err := agents.CanonicalURL(raw.Url)
	if err != nil {
		return ""
	}
	if raw.Hints != nil && strings.TrimSpace(raw.Hints.EventId) != "" && strings.TrimSpace(raw.Hints.Revision) != "" {
		u += "#event=" + strings.TrimSpace(raw.Hints.EventId) + "@" + strings.TrimSpace(raw.Hints.Revision)
	}
	return agents.ArticleID(u)
}
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is the testable command entry point. Every path refuses an empty NATS
// target and an unbound/mismatched environment before it can fence or write.
func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("repair-wire-time", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbURL := fs.String("database", os.Getenv("DOR_DATABASE_URL"), "Postgres URL")
	natsURL := fs.String("nats", "", "explicit NATS URL of an already provisioned isolated deployment (required; no embedded server)")
	environmentID := fs.String("environment-id", "", "explicit isolated environment id shared by the copy DB, publisher KV and participant leases (required)")
	bindEnvironment := fs.Bool("bind-environment", false, "explicitly bind the non-dor copy DB and publisher store to --environment-id, then exit")
	apply := fs.Bool("apply", false, "apply candidate changes to an explicitly named copy only")
	allowDB := fs.String("allow-db", "", "exact copy database name; dor is forbidden")
	confirm := fs.Bool("confirm-candidates", false, "operator has verified each retained raw fetch time is the original; also authorizes a plan override")
	planOut := fs.String("plan-out", "", "write the deterministic revision-bound maintenance plan to this file (read-only)")
	maintenanceApply := fs.Bool("maintenance-apply", false, "run the full fenced maintenance protocol against an explicit non-dor copy")
	var expectedProducers, excludedProducers, expectedGateways, excludedGateways, expectedConsumers stringList
	fs.Var(&expectedProducers, "expected-producer", "runtime id that must be paused and drained (repeatable)")
	fs.Var(&excludedProducers, "excluded-producer", "runtime id explicitly excluded by external routing evidence (repeatable)")
	fs.Var(&expectedGateways, "expected-gateway", "gateway id that must be fenced (repeatable)")
	fs.Var(&excludedGateways, "excluded-gateway", "gateway id explicitly excluded by external routing evidence (repeatable)")
	fs.Var(&expectedConsumers, "expected-consumer", "durable consumer STREAM/name that must be present (repeatable)")
	exclusionAuthority := fs.String("exclusion-authority", "", "routing/process authority that excluded absent participants")
	exclusionEvidence := fs.String("exclusion-evidence", "", "machine evidence id from the exclusion authority")
	exclusionGeneration := fs.Uint64("exclusion-generation", 0, "monotonic exclusion generation from the authority")
	drainTimeout := fs.Duration("drain-timeout", 2*time.Minute, "maximum time to wait for a stable finite backlog drain")
	if err := fs.Parse(args); err != nil {
		return err
	}

	dbTarget := strings.TrimSpace(*dbURL)
	if dbTarget == "" {
		return errors.New("database URL required")
	}
	natsTarget := strings.TrimSpace(*natsURL)
	if natsTarget == "" {
		return errors.New("explicit non-empty --nats target is required; this command never starts or provisions an embedded server")
	}
	environment, err := maintenance.NormalizeEnvironmentID(*environmentID)
	if err != nil {
		return err
	}
	allowCopy := strings.TrimSpace(*allowDB)
	if *bindEnvironment {
		if *apply || *maintenanceApply || *planOut != "" {
			return errors.New("--bind-environment is a separate explicit setup step; do not combine it with --apply, --maintenance-apply or --plan-out")
		}
	}

	ctx := context.Background()
	db, err := pgx.Connect(ctx, dbTarget)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	// Attach never starts an embedded server and never provisions streams/KV.
	b, err := bus.Attach(ctx, bus.Options{URL: natsTarget, EnvironmentID: environment})
	if err != nil {
		return err
	}
	defer b.Close()
	publisherKV, err := b.KV(ctx, bus.KVBucketPublisher)
	if err != nil {
		return err
	}
	gatewayLeases, err := b.KV(ctx, bus.KVBucketGatewayLease)
	if err != nil {
		return err
	}
	producerLeases, err := b.KV(ctx, bus.KVBucketProducerLease)
	if err != nil {
		return err
	}

	dbEnvironment, err := readDatabaseEnvironment(ctx, db)
	if err != nil {
		return err
	}
	pubEnvironment, _, err := maintenance.ReadEnvironment(ctx, publisherKV)
	if err != nil {
		return err
	}

	if *bindEnvironment {
		// Refuse conflicting durable sides before writing either one. A
		// partially completed bind is resumable because both sides are
		// insert-once and equal-only.
		if dbEnvironment != "" && dbEnvironment != environment {
			return fmt.Errorf("database is already bound to environment %q, not %q", dbEnvironment, environment)
		}
		if pubEnvironment.ID != "" && pubEnvironment.ID != environment {
			return fmt.Errorf("publisher is already bound to environment %q, not %q", pubEnvironment.ID, environment)
		}
		name, err := bindDatabaseEnvironment(ctx, db, allowCopy, environment)
		if err != nil {
			return err
		}
		if err := maintenance.PutEnvironment(ctx, publisherKV, environment); err != nil {
			return err
		}
		boundDB, err := readDatabaseEnvironment(ctx, db)
		if err != nil {
			return err
		}
		if err := maintenance.RequireEnvironment("database", boundDB, environment); err != nil {
			return err
		}
		boundPub, revision, err := maintenance.ReadEnvironment(ctx, publisherKV)
		if err != nil {
			return err
		}
		if revision == 0 {
			return errors.New("maintenance: publisher environment identity is unbound after binding")
		}
		if err := maintenance.RequireEnvironment("publisher", boundPub.ID, environment); err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(report{
			Database:         name,
			Environment:      environment,
			EnvironmentBound: true,
			Warning:          "Explicit environment binding only: no audit, candidate repair or maintenance fence was performed.",
		})
	}

	// Every audit/apply/resume compares the durable targets before the first
	// audit read result can drive a fence or write.
	if err := maintenance.RequireEnvironment("database", dbEnvironment, environment); err != nil {
		return err
	}
	if err := maintenance.RequireEnvironment("publisher", pubEnvironment.ID, environment); err != nil {
		return err
	}
	if err := maintenance.ValidateParticipantEnvironments(ctx, gatewayLeases, producerLeases, environment); err != nil {
		return err
	}

	auditRun, err := auditRetained(ctx, db, b)
	if err != nil {
		return err
	}
	name, asOf, changes, info := auditRun.name, auditRun.asOf, auditRun.changes, auditRun.stream
	if *apply && (allowCopy == "" || allowCopy != name || name == "dor") {
		return errors.New("apply requires --allow-db matching a non-dor copy")
	}
	if *apply && !*maintenanceApply && !*confirm {
		return errors.New("DB-only apply requires --confirm-candidates")
	}
	if *maintenanceApply {
		if !*apply {
			return errors.New("--maintenance-apply requires --apply")
		}
		if len(expectedProducers) == 0 || len(expectedGateways) == 0 || len(expectedConsumers) == 0 {
			return errors.New("--maintenance-apply requires nonempty expected producer, gateway and consumer inventories")
		}
	}
	out := report{Database: name, Environment: environment, AsOf: asOf, IngestFirst: info.State.FirstSeq, IngestLast: info.State.LastSeq, Changes: changes, Warning: "Raw retention is limited; later refetch is not necessarily the original. Publisher snapshots, journal and derived state require a separate fenced reconciliation; no live cutover is authorized."}
	if *planOut != "" {
		audit := make([]maintenance.AuditArticle, 0, len(changes))
		for _, c := range changes {
			audit = append(audit, maintenance.AuditArticle{
				ID:         c.ID,
				Published:  c.Published.Unix(),
				Created:    c.Created.Unix(),
				StoryID:    c.StoryID,
				FetchTimes: c.FetchTimes,
			})
		}
		plan, err := maintenance.BuildPlan(ctx, b, name, asOf, audit, *confirm)
		if err != nil {
			return err
		}
		data, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*planOut, append(data, '\n'), 0o600); err != nil {
			return err
		}
		out.Plan = plan
		if !*confirm {
			out.Warning += " The exported plan suppresses every audited identity because --confirm-candidates was not supplied."
		}
	}
	audit := auditFromChanges(changes)
	if *maintenanceApply {
		exclusion := maintenance.ExclusionProof{
			Version:           maintenance.ControlVersion,
			Authority:         *exclusionAuthority,
			EvidenceID:        *exclusionEvidence,
			Generation:        *exclusionGeneration,
			Expected:          expectedGateways,
			Excluded:          excludedGateways,
			ExpectedProducers: expectedProducers,
			ExcludedProducers: excludedProducers,
		}
		exclusion.Digest = maintenance.ExclusionDigest(exclusion.Authority, exclusion.EvidenceID, exclusion.Generation, exclusion.Expected, exclusion.Excluded, exclusion.ExpectedProducers, exclusion.ExcludedProducers)
		if err := maintenance.ValidateExclusionProof(exclusion); err != nil {
			return err
		}
		coord, err := maintenance.NewCoordinator(ctx, b, environment)
		if err != nil {
			return err
		}
		result, err := coord.Apply(ctx, maintenance.ApplyRequest{
			Database:          name,
			EnvironmentID:     environment,
			AsOf:              asOf,
			Audit:             audit,
			ConfirmOriginals:  *confirm,
			ExpectedProducers: expectedProducers,
			ExcludedProducers: excludedProducers,
			ExpectedGateways:  expectedGateways,
			ExcludedGateways:  excludedGateways,
			ExpectedConsumers: expectedConsumers,
			Exclusion:         exclusion,
			DrainTimeout:      *drainTimeout,
			VerifyEnvironment: func(callCtx context.Context) error {
				current, verr := readDatabaseEnvironment(callCtx, db)
				if verr != nil {
					return verr
				}
				return maintenance.RequireEnvironment("database", current, environment)
			},
			AuditFunc: func(callCtx context.Context) (time.Time, []maintenance.AuditArticle, error) {
				run, aerr := auditRetained(callCtx, db, b)
				if aerr != nil {
					return time.Time{}, nil, aerr
				}
				out.Changes, out.AsOf = run.changes, run.asOf
				if run.stream != nil {
					out.IngestFirst, out.IngestLast = run.stream.State.FirstSeq, run.stream.State.LastSeq
				}
				return run.asOf, auditFromChanges(run.changes), nil
			},
			DBRepair: func(callCtx context.Context, plan *maintenance.Plan) (*maintenance.DBRepairResult, error) {
				if !strings.EqualFold(plan.Database, name) || strings.EqualFold(plan.Database, "dor") {
					return nil, errors.New("maintenance DB callback database identity changed")
				}
				return applyCandidates(callCtx, db, name, environment, planCandidates(plan))
			},
		})
		if err != nil {
			return err
		}
		out.Maintenance = result
		out.Committed = true
		return json.NewEncoder(stdout).Encode(out)
	}
	if *apply {
		result, err := applyCandidates(ctx, db, name, environment, changes)
		if err != nil {
			return err
		}
		out.Committed = result != nil
	}
	return json.NewEncoder(stdout).Encode(out)
}

type auditRunResult struct {
	name    string
	asOf    time.Time
	changes []*candidate
	stream  *jetstream.StreamInfo
}

// auditRetained reads the DB future-time census and matches retained INGEST
// raw evidence. It is called again after the producer/consumer drain so the
// plan covers every article that settled during the finite backlog window.
func auditRetained(ctx context.Context, db *pgx.Conn, b *bus.Bus) (*auditRunResult, error) {
	var name string
	var asOf time.Time
	if err := db.QueryRow(ctx, "SELECT current_database(), statement_timestamp()").Scan(&name, &asOf); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, "SELECT id, published_at, created_at, coalesce(story_id,'') FROM articles WHERE published_at > $1 ORDER BY id, created_at", asOf)
	if err != nil {
		return nil, err
	}
	var changes []*candidate
	byID := map[string][]*candidate{}
	for rows.Next() {
		c := &candidate{}
		if err = rows.Scan(&c.ID, &c.Published, &c.Created, &c.StoryID); err != nil {
			return nil, err
		}
		changes = append(changes, c)
		byID[c.ID] = append(byID[c.ID], c)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	suppressed, err := tx.Query(ctx, "SELECT article_id, observed_published_at FROM wire_time_suppression ORDER BY article_id")
	if err != nil {
		return nil, err
	}
	for suppressed.Next() {
		c := &candidate{}
		if err = suppressed.Scan(&c.ID, &c.Published); err != nil {
			return nil, err
		}
		c.Created = c.Published
		c.Reason = "existing audited suppression"
		if _, ok := byID[c.ID]; !ok {
			changes = append(changes, c)
			byID[c.ID] = append(byID[c.ID], c)
		}
	}
	if err = suppressed.Err(); err != nil {
		return nil, err
	}
	suppressed.Close()
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	stream, err := b.StreamNamed(ctx, bus.StreamIngest)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && seq > 0; seq++ {
		msg, e := stream.GetMsg(ctx, seq)
		if errors.Is(e, jetstream.ErrMsgNotFound) {
			continue
		}
		if e != nil {
			return nil, e
		}
		var raw dorv1.RawArticle
		if e = proto.Unmarshal(msg.Data, &raw); e != nil {
			return nil, fmt.Errorf("raw sequence %d: %w", seq, e)
		}
		for _, c := range byID[rawID(&raw)] {
			if raw.PublishedAt == c.Published.Unix() && raw.FetchedAt > 0 && raw.FetchedAt < raw.PublishedAt {
				found := false
				for _, v := range c.FetchTimes {
					if v == raw.FetchedAt {
						found = true
					}
				}
				if !found {
					c.FetchTimes = append(c.FetchTimes, raw.FetchedAt)
				}
			}
		}
	}
	for _, c := range changes {
		if len(c.FetchTimes) == 1 {
			c.RepairAt = c.FetchTimes[0]
			c.Reason = "candidate; confirm original fetch identity before use"
		} else {
			c.Reason = "unknown or conflicting raw fetch time; suppress retained presentation"
		}
	}
	return &auditRunResult{name: name, asOf: asOf, changes: changes, stream: info}, nil
}

func auditFromChanges(changes []*candidate) []maintenance.AuditArticle {
	audit := make([]maintenance.AuditArticle, 0, len(changes))
	for _, c := range changes {
		audit = append(audit, maintenance.AuditArticle{
			ID:         c.ID,
			Published:  c.Published.Unix(),
			Created:    c.Created.Unix(),
			StoryID:    c.StoryID,
			FetchTimes: c.FetchTimes,
		})
	}
	return audit
}

func planCandidates(plan *maintenance.Plan) []*candidate {
	out := make([]*candidate, 0, len(plan.Articles))
	for _, item := range plan.Articles {
		out = append(out, &candidate{
			ID:        item.ArticleID,
			Published: time.Unix(item.ObservedPublishedAt, 0).UTC(),
			Created:   time.Unix(item.CreatedAt, 0).UTC(),
			RepairAt:  item.OverrideAt,
			Reason:    item.Reason,
		})
	}
	return out
}

// applyCandidates is the copy-only DB transaction shared by the audit command
// and the maintenance adapter. It is idempotent: a retry after an unknown
// commit accepts an already-repaired row and verifies it by readback. The
// database environment must still match the explicit target before any row is
// locked or changed.
func applyCandidates(ctx context.Context, db *pgx.Conn, name, environmentID string, changes []*candidate) (*maintenance.DBRepairResult, error) {
	var current string
	if err := db.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		return nil, err
	}
	if current != name || strings.EqualFold(current, "dor") {
		return nil, errors.New("apply refuses a database other than the bound non-dor copy")
	}
	bound, err := readDatabaseEnvironment(ctx, db)
	if err != nil {
		return nil, err
	}
	if err := maintenance.RequireEnvironment("database", bound, environmentID); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "LOCK TABLE articles IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return nil, err
	}
	out := &maintenance.DBRepairResult{Database: name}
	for _, c := range changes {
		if c.RepairAt == 0 {
			// Preserve the article and raw evidence; suppress its presentation.
			var already bool
			if e := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM wire_time_suppression WHERE article_id=$1)", c.ID).Scan(&already); e != nil {
				return nil, e
			}
			if !already {
				if _, e := tx.Exec(ctx, `INSERT INTO wire_time_suppression (article_id, observed_published_at, reason, audited_at)
SELECT id, published_at, $3, $4 FROM articles WHERE id=$1 AND published_at=$2 AND created_at=$5
ON CONFLICT (article_id) DO UPDATE SET observed_published_at=EXCLUDED.observed_published_at, reason=EXCLUDED.reason, audited_at=EXCLUDED.audited_at`, c.ID, c.Published, c.Reason, time.Now().UTC(), c.Created); e != nil {
					return nil, e
				}
			}
			out.Rows = append(out.Rows, maintenance.DBRowOutcome{
				ArticleID:       c.ID,
				BeforePublished: c.Published.Unix(),
				BeforeCreated:   c.Created.Unix(),
				AfterPublished:  c.Published.Unix(),
				Suppressed:      true,
			})
			continue
		}
		t := time.Unix(c.RepairAt, 0).UTC()
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM articles WHERE id=$1)", c.ID).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			out.Rows = append(out.Rows, maintenance.DBRowOutcome{
				ArticleID:       c.ID,
				BeforePublished: c.Published.Unix(),
				BeforeCreated:   c.Created.Unix(),
				AfterPublished:  t.Unix(),
			})
			continue
		}
		if _, err = tx.Exec(ctx, "DELETE FROM wire_time_suppression WHERE article_id=$1", c.ID); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, "SELECT ensure_article_partition($1::date)", t.Format("2006-01-02")); err != nil {
			return nil, err
		}
		tag, e := tx.Exec(ctx, `UPDATE articles SET published_at=$1, created_at=$1
WHERE id=$2 AND ((published_at=$3 AND created_at=$4) OR (published_at=$1 AND created_at=$1))`, t, c.ID, c.Published, c.Created)
		if e != nil {
			return nil, e
		}
		if tag.RowsAffected() != 1 {
			var pub, created time.Time
			if e = tx.QueryRow(ctx, "SELECT published_at, created_at FROM articles WHERE id=$1", c.ID).Scan(&pub, &created); e != nil {
				return nil, e
			}
			if !pub.Equal(t) || !created.Equal(t) {
				return nil, errors.New("article changed since audit; transaction aborted")
			}
		}
		out.Rows = append(out.Rows, maintenance.DBRowOutcome{
			ArticleID:       c.ID,
			BeforePublished: c.Published.Unix(),
			BeforeCreated:   c.Created.Unix(),
			AfterPublished:  t.Unix(),
		})
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if err = readbackCandidates(ctx, db, out.Rows); err != nil {
		return nil, err
	}
	out.CommittedAt = time.Now().UnixNano()
	out.ContentHash = maintenance.HashDBRows(out.Database, out.Rows)
	return out, nil
}

func readbackCandidates(ctx context.Context, db *pgx.Conn, rows []maintenance.DBRowOutcome) error {
	for _, row := range rows {
		if row.Suppressed {
			var ok bool
			if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM wire_time_suppression WHERE article_id=$1)", row.ArticleID).Scan(&ok); err != nil {
				return err
			}
			if !ok {
				var exists bool
				if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM articles WHERE id=$1)", row.ArticleID).Scan(&exists); err == nil && exists {
					return errors.New("suppression readback missing")
				}
			}
			continue
		}
		var pub, created time.Time
		err := db.QueryRow(ctx, "SELECT published_at, created_at FROM articles WHERE id=$1", row.ArticleID).Scan(&pub, &created)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		var suppressed bool
		if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM wire_time_suppression WHERE article_id=$1)", row.ArticleID).Scan(&suppressed); err != nil {
			return err
		}
		if suppressed {
			return errors.New("timestamp readback still suppressed after override")
		}
		want := time.Unix(row.AfterPublished, 0).UTC()
		if !pub.Equal(want) || !created.Equal(want) {
			return errors.New("timestamp readback mismatch after commit")
		}
	}
	return nil
}
