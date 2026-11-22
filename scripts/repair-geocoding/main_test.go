package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ankojh/dor/internal/geo"
	"github.com/jackc/pgx/v5"
)

type fixedDatabaseRow struct {
	name string
	err  error
}

func (r fixedDatabaseRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return errors.New("expected one scan destination")
	}
	name, ok := dest[0].(*string)
	if !ok {
		return errors.New("expected string scan destination")
	}
	*name = r.name
	return nil
}

type fixedDatabase struct{ row pgx.Row }

func (db fixedDatabase) QueryRow(context.Context, string, ...any) pgx.Row { return db.row }

func TestApplyDatabaseGateBlocksLiveDatabaseUnconditionally(t *testing.T) {
	ctx := context.Background()
	if err := validateApplyDatabase(ctx, fixedDatabase{row: fixedDatabaseRow{name: "dor"}}, "dor"); err == nil || !strings.Contains(err.Error(), "live database dor is blocked") {
		t.Fatalf("live database must fail the unconditional safety gate even if explicitly allowed, got %v", err)
	}
	if err := validateApplyDatabase(ctx, fixedDatabase{row: fixedDatabaseRow{name: "test_db"}}, ""); err == nil {
		t.Fatal("empty allow-list name must fail")
	}
	if err := validateApplyDatabase(ctx, fixedDatabase{row: fixedDatabaseRow{name: "test_db"}}, "other_db"); err == nil {
		t.Fatal("database mismatch must fail")
	}
	queryErr := errors.New("current_database unavailable")
	if err := validateApplyDatabase(ctx, fixedDatabase{row: fixedDatabaseRow{err: queryErr}}, "test_db"); !errors.Is(err, queryErr) {
		t.Fatalf("database identity query error should propagate, got %v", err)
	}
}

func TestParseOptionsRequiresExplicitWriteAuthorization(t *testing.T) {
	for _, args := range [][]string{
		{"--dsn", "postgres://example", "--dry-run=false"},
		{"--dsn", "postgres://example", "--dry-run=false", "--apply=false"},
		{"--dsn", "postgres://example", "--apply"},
		{"--dsn", "postgres://example", "--apply", "--allow-db", "dor"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Errorf("parseOptions(%q) should reject write-like dry-run setting without --apply", args)
		}
	}
	parsed, err := parseOptions([]string{"--dsn", "postgres://example", "--apply", "--allow-db", "dor_f1_1_sql_test"})
	if err != nil || !parsed.apply {
		t.Fatalf("explicit --apply should enable writes: options=%+v err=%v", parsed, err)
	}
	parsed, err = parseOptions([]string{"--dsn", "postgres://example"})
	if err != nil || parsed.apply {
		t.Fatalf("default options should be read-only: options=%+v err=%v", parsed, err)
	}
}

func TestRunReportRejectsMissingDatasetBeforeDatabaseConnect(t *testing.T) {
	empty := t.TempDir()
	err := runReport(context.Background(), "postgres://not-reached", empty, false, "", "")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "cities15000.txt") {
		t.Fatalf("missing local GeoNames dataset should fail before connecting to database, got %v", err)
	}
}

func TestRepairDatabaseFlow(t *testing.T) {
	dsn := repairTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, dsn)
	defer conn.Close(ctx)
	if err := validateApplyDatabase(ctx, conn, "dor_f1_1_sql_test"); err != nil {
		t.Fatalf("exact isolated database name should authorize apply: %v", err)
	}
	if err := validateApplyDatabase(ctx, conn, "another_db"); err == nil {
		t.Fatal("database name mismatch should fail the apply gate")
	}
	if err := validateApplyDatabase(ctx, conn, "dor"); err == nil {
		t.Fatal("live database name must never pass the apply gate")
	}

	proposed, err := repair(ctx, conn, g, false)
	if err != nil {
		t.Fatalf("dry-run repair: %v", err)
	}
	if proposed.Status != "proposed" || proposed.Committed || proposed.Tables["story_locations"].Flagged != 1 || proposed.Tables["article_locations"].Flagged != 2 {
		t.Fatalf("dry run must report proposals without claiming commit: %+v", proposed)
	}
	repeatedProposal, err := repair(ctx, conn, g, false)
	if err != nil || repeatedProposal.Bundle.RunID != proposed.Bundle.RunID {
		t.Fatalf("identical DB proposal must have deterministic run identity: first=%q second=%q err=%v", proposed.Bundle.RunID, repeatedProposal.Bundle.RunID, err)
	}
	assertOriginalRepairRows(t, ctx, conn)

	committed, err := repair(ctx, conn, g, true)
	if err != nil {
		t.Fatalf("apply repair: %v", err)
	}
	if committed.Status != "committed" || !committed.Committed || len(committed.Changes) != 3 {
		t.Fatalf("successful apply must return committed audit with all changes: %+v", committed)
	}
	if len(committed.Bundle.Stories) != 1 || len(committed.Bundle.Articles) != 2 {
		t.Fatalf("bundle must carry affected story and article arrays: %+v", committed.Bundle)
	}
	story := committed.Bundle.Stories[0]
	if story.PrimaryPlaceID != "paris" || story.Unlocated || len(story.Locations) != 2 {
		t.Fatalf("planned story bundle has wrong collision/primary result: %+v", story)
	}
	parisVote, ok := voteByID(story.Locations, "paris")
	if !ok || parisVote.Weight != .8 {
		t.Fatalf("collision must preserve highest story weight: %+v", story.Locations)
	}
	article := articlePatchByID(t, committed.Bundle.Articles, "article-1")
	articleParis, ok := voteByID(article.Locations, "paris")
	if !ok || articleParis.Confidence != .9 {
		t.Fatalf("article collision must preserve highest confidence: %+v", article.Locations)
	}
	if article.EventCandidate.Place != "Paris" || !article.EventCandidate.Located || math.Abs(article.EventCandidate.Lat-48.8566) > 1e-5 || !article.NeedsHotEvidence {
		t.Fatalf("article event candidate must use repaired story primary and require hot evidence: %+v", article)
	}
	unchangedStoryArticle := articlePatchByID(t, committed.Bundle.Articles, "article-2")
	if len(unchangedStoryArticle.Locations) != 0 || !unchangedStoryArticle.Unlocated || !unchangedStoryArticle.NeedsHotEvidence {
		t.Fatalf("empty article votes on an unchanged located story must still need hot evidence: %+v", unchangedStoryArticle)
	}
	if !unchangedStoryArticle.EventCandidate.Located || unchangedStoryArticle.EventCandidate.Place != "Boston" || math.Abs(unchangedStoryArticle.EventCandidate.Lat-42.3601) > 1e-5 {
		t.Fatalf("empty article votes should retain unchanged story primary as event candidate: %+v", unchangedStoryArticle.EventCandidate)
	}
	assertBundleMakesNoFinalWireClaim(t, committed.Bundle)
	assertCommittedRepairRows(t, ctx, conn)

	auditPath := filepath.Join(t.TempDir(), "audit.json")
	if err := writeReport(auditPath, committed); err != nil {
		t.Fatalf("write committed audit: %v", err)
	}
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted report
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("decode audit: %v", err)
	}
	if persisted.Status != "committed" || !persisted.Committed || persisted.Bundle.RunID != committed.Bundle.RunID || len(persisted.Changes) != 3 {
		t.Fatalf("persisted audit does not describe successful SQL commit: %+v", persisted)
	}

	second, err := repair(ctx, conn, g, true)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(second.Changes) != 0 || second.Tables["story_locations"].Flagged != 0 || second.Tables["article_locations"].Flagged != 0 {
		t.Fatalf("second repair should be idempotent: %+v", second)
	}
}

func TestRepairCommitFailureRollsBackAndReturnsNoCommittedAudit(t *testing.T) {
	dsn := repairTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, dsn)
	defer conn.Close(ctx)
	result, err := repair(ctx, failCommitStarter{Conn: conn}, g, true)
	if err == nil || !strings.Contains(err.Error(), "injected commit failure") {
		t.Fatalf("repair should surface commit failure, result=%+v err=%v", result, err)
	}
	if result.Committed || result.Status != "" || len(result.Changes) != 0 {
		t.Fatalf("failed commit must not return success audit: %+v", result)
	}
	assertOriginalRepairRows(t, ctx, conn)
}

func TestRepairAuditReportsPreservedAmbiguousCanonicalLocations(t *testing.T) {
	dsn := repairTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, g := prepareRepairDB(t, ctx, dsn)
	defer conn.Close(ctx)
	statements := []string{
		`INSERT INTO public.stories(id,title,primary_loc) VALUES ('story-review','Hyderabad Announces a New Policy','hyderabad-pk')`,
		`INSERT INTO public.articles(id,story_id,title,lede,dateline) VALUES ('article-review','story-review','Hyderabad Announces a New Policy','','')`,
		`INSERT INTO public.story_locations VALUES ('story-review','hyderabad-pk','Hyderabad',25.395999908447266,68.35780334472656,2,0,.6)`,
		`INSERT INTO public.article_locations VALUES ('article-review','hyderabad-pk','Hyderabad',25.395999908447266,68.35780334472656,2,0,.6)`,
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed ambiguous historical location: %v", err)
		}
	}

	audit, err := repair(ctx, conn, g, false)
	if err != nil {
		t.Fatalf("dry-run ambiguous-location audit: %v", err)
	}
	if audit.Tables["story_locations"].NeedsReview != 1 || audit.Tables["article_locations"].NeedsReview != 1 {
		t.Fatalf("ambiguous canonical locations must increment needs_review without proposing deletion: story=%+v article=%+v", audit.Tables["story_locations"], audit.Tables["article_locations"])
	}
	for _, want := range []struct{ table, owner string }{{"story_locations", "story-review"}, {"article_locations", "article-review"}} {
		found := false
		for _, review := range audit.Reviews {
			if review.Table == want.table && review.Owner == want.owner {
				found = true
				if review.PlaceID != "hyderabad-pk" || review.Reason != "canonical_title_requires_disambiguation" {
					t.Errorf("unexpected review record for %s/%s: %+v", want.table, want.owner, review)
				}
			}
		}
		if !found {
			t.Errorf("missing structured review for %s/%s: %+v", want.table, want.owner, audit.Reviews)
		}
	}
	for _, change := range audit.Changes {
		if change.Owner == "story-review" || change.Owner == "article-review" {
			t.Errorf("review-only location must not appear as a destructive/replot change: %+v", change)
		}
	}
	for _, story := range audit.Bundle.Stories {
		if story.ID == "story-review" {
			t.Errorf("review-only story must not be placed in deletion/replacement bundle: %+v", story)
		}
	}
	for _, article := range audit.Bundle.Articles {
		if article.ID == "article-review" {
			t.Errorf("review-only article must not be placed in deletion/replacement bundle: %+v", article)
		}
	}
	for _, table := range []string{"story_locations", "article_locations"} {
		ownerCol := "story_id"
		if table == "article_locations" {
			ownerCol = "article_id"
		}
		var count int
		query := `SELECT count(*) FROM public.` + table + ` WHERE ` + ownerCol + `=$1 AND place_id='hyderabad-pk'`
		owner := "story-review"
		if table == "article_locations" {
			owner = "article-review"
		}
		if err := conn.QueryRow(ctx, query, owner).Scan(&count); err != nil || count != 1 {
			t.Errorf("dry-run must leave the reviewed canonical location present in %s: count=%d err=%v", table, count, err)
		}
	}
}

func TestWriteReportReplacesFileWithCommittedOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte("old report"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := report{Status: "committed", Committed: true, Tables: map[string]*counts{}}
	if err := writeReport(path, want); err != nil {
		t.Fatalf("write report: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(data, &got); err != nil || got.Status != "committed" || !got.Committed {
		t.Fatalf("report content = %s, decoded %+v, err %v", data, got, err)
	}
}

func repairTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DOR_TEST_REPAIR_DSN")
	if dsn == "" {
		t.Skip("set DOR_TEST_REPAIR_DSN to the isolated dor_f1_1_sql_test database")
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database DSN: %v", err)
	}
	if config.Database != "dor_f1_1_sql_test" {
		t.Fatalf("refusing to touch database %q; expected isolated dor_f1_1_sql_test", config.Database)
	}
	return dsn
}

func prepareRepairDB(t *testing.T, ctx context.Context, dsn string) (*pgx.Conn, *geo.Gazetteer) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to isolated repair test database: %v", err)
	}
	statements := []string{
		`DROP TABLE IF EXISTS public.article_locations, public.story_locations, public.articles, public.stories CASCADE`,
		`CREATE TABLE public.stories (id text PRIMARY KEY, title text NOT NULL, primary_loc text NOT NULL DEFAULT '')`,
		`CREATE TABLE public.articles (id text PRIMARY KEY, story_id text NOT NULL, title text NOT NULL, lede text NOT NULL DEFAULT '', dateline text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now())`,
		`CREATE TABLE public.story_locations (story_id text NOT NULL, place_id text NOT NULL, name text NOT NULL, lat double precision NOT NULL, lon double precision NOT NULL, precision smallint NOT NULL, role smallint NOT NULL, weight real NOT NULL, UNIQUE(story_id,place_id,role))`,
		`CREATE TABLE public.article_locations (article_id text NOT NULL, place_id text NOT NULL, name text NOT NULL, lat double precision NOT NULL, lon double precision NOT NULL, precision smallint NOT NULL, role smallint NOT NULL, conf real NOT NULL, UNIQUE(article_id,place_id,role))`,
		`INSERT INTO public.stories(id,title,primary_loc) VALUES ('story-1','Officials meet in Paris','old'), ('story-2','Officials gather in Boston','boston')`,
		`INSERT INTO public.articles(id,story_id,title,lede,dateline) VALUES ('article-1','story-1','Officials meet in Paris','Delegates gathered at Boston.','Paris, France'), ('article-2','story-2','Unlocated report','No place evidence here.','')`,
		`INSERT INTO public.story_locations VALUES ('story-1','old','Unknown',0,0,2,0,.5), ('story-1','paris','Paris',48.85660171508789,2.3522000312805176,2,0,.8), ('story-1','boston','Boston',42.36009979248047,-71.05889892578125,2,0,.7), ('story-2','boston','Boston',42.36009979248047,-71.05889892578125,2,0,.6)`,
		`INSERT INTO public.article_locations VALUES ('article-1','old','Unknown',0,0,2,0,.4), ('article-1','paris','Paris',48.85660171508789,2.3522000312805176,2,0,.9), ('article-1','boston','Boston',42.36009979248047,-71.05889892578125,2,0,.7), ('article-2','old','Unknown',0,0,2,0,.4)`,
	}
	for _, stmt := range statements {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			conn.Close(ctx)
			t.Fatalf("create test tables and rows: %v", err)
		}
	}
	g := repairFixtureGazetteer(t)
	return conn, g
}

func repairFixtureGazetteer(t *testing.T) *geo.Gazetteer {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"countryInfo.txt":      "US\tUSA\t\t\tUnited States\tWashington\nFR\tFRA\t\t\tFrance\tParis\nIN\tIND\t\t\tIndia\tNew Delhi\nPK\tPAK\t\t\tPakistan\tIslamabad\n",
		"admin1CodesASCII.txt": "US.MA\tMassachusetts\nFR.11\tIle-de-France\nIN.TG\tTelangana\nPK.SD\tSindh\n",
		"cities15000.txt": strings.Join([]string{
			geoRow("paris", "Paris", "PPLC", "48.8566", "2.3522", "FR", "11", "2161000"),
			geoRow("boston", "Boston", "PPLA", "42.3601", "-71.0589", "US", "MA", "675000"),
			geoRow("washington", "Washington", "PPLC", "38.9072", "-77.0369", "US", "DC", "700000"),
			geoRow("hyderabad-in", "Hyderabad", "PPLA", "17.3850", "78.4867", "IN", "TG", "10000000"),
			geoRow("hyderabad-pk", "Hyderabad", "PPLA", "25.3960", "68.3578", "PK", "SD", "1000000"),
		}, ""),
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	g, err := geo.LoadExisting(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("load fixture geonames: %v", err)
	}
	return g
}

func geoRow(id, name, feature, lat, lon, country, admin, population string) string {
	fields := make([]string, 19)
	fields[0], fields[1], fields[4], fields[5], fields[7] = id, name, lat, lon, feature
	fields[8], fields[10], fields[14] = country, admin, population
	return strings.Join(fields, "\t") + "\n"
}

func assertOriginalRepairRows(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	var count int
	var primary string
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM public.story_locations WHERE story_id='story-1' AND place_id='old'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("dry/failed repair changed original story location count=%d err=%v", count, err)
	}
	if err := conn.QueryRow(ctx, `SELECT primary_loc FROM public.stories WHERE id='story-1'`).Scan(&primary); err != nil || primary != "old" {
		t.Fatalf("dry/failed repair changed primary=%q err=%v", primary, err)
	}
}

func assertCommittedRepairRows(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	var count int
	var weight float32
	var primary string
	if err := conn.QueryRow(ctx, `SELECT count(*),max(weight) FROM public.story_locations WHERE story_id='story-1' AND place_id='paris' AND role=0`).Scan(&count, &weight); err != nil || count != 1 || weight != .8 {
		t.Fatalf("story collision not coalesced at max weight: count=%d weight=%v err=%v", count, weight, err)
	}
	if err := conn.QueryRow(ctx, `SELECT primary_loc FROM public.stories WHERE id='story-1'`).Scan(&primary); err != nil || primary != "paris" {
		t.Fatalf("story primary after apply = %q, want paris (err=%v)", primary, err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*),max(conf) FROM public.article_locations WHERE article_id='article-1' AND place_id='paris' AND role=0`).Scan(&count, &weight); err != nil || count != 1 || weight != .9 {
		t.Fatalf("article collision not coalesced at max confidence: count=%d conf=%v err=%v", count, weight, err)
	}
	for _, table := range []string{"story_locations", "article_locations"} {
		var invalid int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM public.`+table+` WHERE lat=0 AND lon=0`).Scan(&invalid); err != nil || invalid != 0 {
			t.Fatalf("invalid coordinates remain in %s: count=%d err=%v", table, invalid, err)
		}
	}
}

func voteByID(votes []geo.LocationVote, id string) (geo.LocationVote, bool) {
	for _, v := range votes {
		if v.PlaceID == id {
			return v, true
		}
	}
	return geo.LocationVote{}, false
}

func articlePatchByID(t *testing.T, patches []geo.ArticleLocationPatch, id string) geo.ArticleLocationPatch {
	t.Helper()
	for _, patch := range patches {
		if patch.ID == id {
			return patch
		}
	}
	t.Fatalf("article patch %q not found in %+v", id, patches)
	return geo.ArticleLocationPatch{}
}

func assertBundleMakesNoFinalWireClaim(t *testing.T, bundle geo.LocationRepairBundle) {
	t.Helper()
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(data)
	for _, field := range []string{`"wire"`, `"final_wire"`, `"serving_permission"`, `"raw_hints"`} {
		if strings.Contains(encoded, field) {
			t.Fatalf("bundle must not contain final wire/hot-state claim %s: %s", field, encoded)
		}
	}
	if bundle.RevisionBinding != geo.RepairRevisionBinding {
		t.Fatalf("expected explicit unbound hot-state revision marker, got %q", bundle.RevisionBinding)
	}
}

type failCommitStarter struct{ Conn *pgx.Conn }

func (s failCommitStarter) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := s.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return failCommitTx{Tx: tx}, nil
}

type failCommitTx struct{ pgx.Tx }

func (tx failCommitTx) Commit(ctx context.Context) error {
	_ = tx.Tx.Rollback(ctx)
	return errors.New("injected commit failure")
}
