// Command repair-geocoding defaults to an offline read-only audit. --apply
// explicitly commits Postgres changes; it never changes running NATS state.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ankojh/dor/internal/geo"
	"github.com/jackc/pgx/v5"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
)

type location struct {
	Table, Owner, PlaceID, Name, Country string
	Lat, Lon                             float64
	Precision, Role                      int16
	Weight                               float32
	Evidence                             []geo.TextEvidence
}
type counts struct {
	Scanned        int            `json:"scanned"`
	Flagged        int            `json:"flagged"`
	Replotted      int            `json:"replotted"`
	Downgraded     int            `json:"downgraded"`
	CountryUpdated int            `json:"country_updated"`
	Removed        int            `json:"removed"`
	NeedsReview    int            `json:"needs_review"`
	ReviewRemovals int            `json:"review_removals"`
	Reasons        map[string]int `json:"reasons"`
}
type change struct {
	row         location
	decision    geo.RepairDecision
	countryOnly string
}
type auditChange struct {
	Table       string         `json:"table"`
	Owner       string         `json:"owner"`
	OldPlace    string         `json:"old_place"`
	OldName     string         `json:"old_name"`
	Reason      string         `json:"reason"`
	Replacement *geo.Place     `json:"replacement"`
	Precision   geo.Resolution `json:"precision"`
}
type auditReview struct {
	Table   string `json:"table"`
	Owner   string `json:"owner"`
	PlaceID string `json:"place_id"`
	Name    string `json:"name"`
	Reason  string `json:"reason"`
}
type report struct {
	Status         string               `json:"status"`
	Committed      bool                 `json:"committed"`
	Tables         map[string]*counts   `json:"tables"`
	Changes        []auditChange        `json:"changes"`
	Reviews        []auditReview        `json:"reviews"`
	ReviewRemovals []auditReview        `json:"review_removals"`
	Bundle         LocationRepairBundle `json:"bundle"`
}
type options struct {
	dsn, dir, reportPath, allowDB, reviewAllowlist string
	apply                                          bool
}

func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("repair-geocoding", flag.ContinueOnError)
	fs.StringVar(&o.dsn, "dsn", os.Getenv("DOR_DATABASE_URL"), "Postgres DSN (or DOR_DATABASE_URL)")
	fs.StringVar(&o.dir, "geonames-dir", geo.DefaultDir, "complete existing GeoNames cache (never downloaded)")
	fs.StringVar(&o.reportPath, "report", "", "atomic JSON audit and repair bundle output")
	fs.BoolVar(&o.apply, "apply", false, "explicitly commit isolated-copy Postgres repairs")
	fs.StringVar(&o.allowDB, "allow-db", "", "required exact isolated database name for --apply (live dor is refused)")
	fs.StringVar(&o.reviewAllowlist, "review-allowlist", "", "optional table|owner|place_id[|region|keep] reviewed keeps and precision caps")
	dry := fs.Bool("dry-run", true, "read-only proposed audit (default)")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.dsn == "" {
		return o, fmt.Errorf("--dsn or DOR_DATABASE_URL is required")
	}
	if !*dry && !o.apply {
		return o, fmt.Errorf("--dry-run=false does not authorize writes; explicit --apply is required")
	}
	if o.apply && (o.allowDB == "" || o.allowDB == "dor") {
		return o, fmt.Errorf("--apply requires --allow-db naming an isolated copy; live database dor is blocked until coordinated cutover")
	}
	if fs.NArg() != 0 {
		return o, fmt.Errorf("unexpected positional arguments")
	}
	return o, nil
}

// loadReviewAllowlist reads table|owner|place_id[|region|keep] lines. Every entry is
// reviewed maintenance input; an empty or missing file removes all review
// candidates by default.
func loadReviewAllowlist(path string) (map[string]bool, error) {
	allow := map[string]bool{}
	if path == "" {
		return allow, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read review allowlist: %w", err)
	}
	for lineNo, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "|")
		if (len(parts) != 3 && len(parts) != 4) || parts[0] != "story_locations" && parts[0] != "article_locations" || parts[1] == "" || parts[2] == "" || len(parts) == 4 && parts[3] != "region" && parts[3] != "keep" {
			return nil, fmt.Errorf("review allowlist line %d must be table|owner|place_id[|region|keep] with a supported table", lineNo+1)
		}
		key := parts[0] + "|" + parts[1] + "|" + parts[2]
		allow[key] = true
		if len(parts) == 4 {
			allow[key+"|"+parts[3]] = true
		}
	}
	return allow, nil
}

func main() {
	o, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := runReport(ctx, o.dsn, o.dir, o.apply, o.reportPath, o.allowDB, o.reviewAllowlist); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func runReport(ctx context.Context, dsn, dir string, apply bool, path, allowDB, reviewAllowlist string) error {
	allowKeep, err := loadReviewAllowlist(reviewAllowlist)
	if err != nil {
		return err
	}
	g, err := geo.LoadExisting(ctx, dir, slog.Default())
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if apply {
		if err := validateApplyDatabase(ctx, conn, allowDB); err != nil {
			return err
		}
	}
	r, err := repair(ctx, conn, g, apply, allowKeep)
	if err != nil {
		return err
	}
	if path != "" {
		if err := writeReport(path, r); err != nil {
			return fmt.Errorf("repair status=%s; audit write failed: %w", r.Status, err)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Status        string             `json:"status"`
		Committed     bool               `json:"committed"`
		Tables        map[string]*counts `json:"tables"`
		BundleRunID   string             `json:"bundle_run_id"`
		DatasetSHA256 string             `json:"dataset_sha256"`
	}{r.Status, r.Committed, r.Tables, r.Bundle.RunID, r.Bundle.DatasetSHA256})
}

// A DSN can select a different database than its apparent path (for example
// through connection parameters). Bind permission to the connected server's
// actual identity before opening any read-write repair transaction.
func validateApplyDatabase(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, allowDB string) error {
	var actual string
	if err := db.QueryRow(ctx, `SELECT current_database()`).Scan(&actual); err != nil {
		return fmt.Errorf("check apply database: %w", err)
	}
	if actual == "dor" {
		return fmt.Errorf("live database dor is blocked until coordinated cutover; this standalone command cannot apply it")
	}
	if allowDB == "" || actual != allowDB {
		return fmt.Errorf("--allow-db must exactly match current_database(): connected to %q, allowed %q", actual, allowDB)
	}
	return nil
}

func writeReport(path string, r report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".geocoding-audit-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.Write(append(data, '\n')); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

type transactionStarter interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

func repair(ctx context.Context, db transactionStarter, g *geo.Gazetteer, apply bool, allowKeep map[string]bool) (report, error) {
	mode := pgx.ReadOnly
	if apply {
		mode = pgx.ReadWrite
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: mode})
	if err != nil {
		return report{}, err
	}
	defer tx.Rollback(ctx)
	if apply {
		if _, err := tx.Exec(ctx, `LOCK TABLE public.stories, public.story_locations, public.article_locations IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return report{}, err
		}
	}
	locations, err := loadLocations(ctx, tx)
	if err != nil {
		return report{}, err
	}
	r := report{Status: "proposed", Tables: map[string]*counts{}, Reviews: []auditReview{}, ReviewRemovals: []auditReview{}}
	for _, table := range []string{"story_locations", "article_locations"} {
		r.Tables[table] = &counts{Reasons: map[string]int{}}
	}
	var changes []change
	for _, row := range locations {
		c := r.Tables[row.Table]
		c.Scanned++
		d := g.RepairLocation(geo.StoredLocation{PlaceID: row.PlaceID, Lat: row.Lat, Lon: row.Lon, Precision: geo.Resolution(row.Precision)}, row.Evidence)
		keepKey := row.Table + "|" + row.Owner + "|" + row.PlaceID
		if d.ReviewReason != "" {
			review := auditReview{row.Table, row.Owner, row.PlaceID, row.Name, d.ReviewReason}
			c.NeedsReview++
			if allowKeep[keepKey] {
				r.Reviews = append(r.Reviews, review)
			} else {
				// Keep decisions suppress removal only; a kept point can still
				// require a precision downgrade or coordinate refresh.
				r.ReviewRemovals = append(r.ReviewRemovals, review)
				c.ReviewRemovals++
				d = geo.RepairDecision{Reason: "review_required"}
			}
		}
		if allowKeep[keepKey+"|keep"] && (d.Replacement == nil || d.Replacement.ID != row.PlaceID) {
			p, found := g.ByID(row.PlaceID)
			if !found || !geo.ValidCoordinates(row.Lat, row.Lon) || geo.DistanceKM(row.Lat, row.Lon, p.Lat, p.Lon) >= 1 {
				return report{}, fmt.Errorf("reviewed keep %s does not have a valid canonical coordinate", keepKey)
			}
			p.Lat, p.Lon = float64(float32(p.Lat)), float64(float32(p.Lon))
			precision := min(geo.Resolution(row.Precision), geo.PrecisionOf(p))
			d = geo.RepairDecision{}
			if precision != geo.Resolution(row.Precision) || row.Lat != p.Lat || row.Lon != p.Lon {
				d = geo.RepairDecision{Reason: "precision", Replacement: &p, Precision: precision}
			}
		}
		if allowKeep[keepKey] {
			precision, ok := titleContainerPrecision(row)
			if allowKeep[keepKey+"|region"] {
				precision, ok = geo.RegionResolution, true
			}
			if ok && geo.Resolution(row.Precision) > precision && (d.Replacement == nil && d.Reason == "" || d.Replacement != nil && d.Replacement.ID == row.PlaceID) {
				if p, found := g.ByID(row.PlaceID); found {
					p.Lat, p.Lon = float64(float32(p.Lat)), float64(float32(p.Lon))
					d = geo.RepairDecision{Reason: "precision", Replacement: &p, Precision: precision, ReviewReason: d.ReviewReason}
				}
			}
		}
		countryOnly := ""
		if d.Reason == "" && row.Country == "" {
			if p, ok := g.ByID(row.PlaceID); ok && p.CountryCode != "" {
				countryOnly = p.CountryCode
				d = geo.RepairDecision{Reason: "country", Replacement: &p, Precision: geo.Resolution(row.Precision)}
			}
		}
		if d.Reason == "" {
			continue
		}
		changes = append(changes, change{row: row, decision: d, countryOnly: countryOnly})
		r.Changes = append(r.Changes, auditChange{row.Table, row.Owner, row.PlaceID, row.Name, d.Reason, d.Replacement, d.Precision})
		c.Flagged++
		c.Reasons[d.Reason]++
		if countryOnly != "" {
			c.CountryUpdated++
		} else if d.Replacement == nil {
			c.Removed++
		} else if d.Reason == "precision" {
			c.Downgraded++
		} else {
			c.Replotted++
		}
	}
	bundle, err := buildBundle(ctx, tx, g, locations, changes)
	if err != nil {
		return report{}, err
	}
	r.Bundle = bundle
	if !apply {
		return r, nil
	}
	for _, c := range changes {
		if c.countryOnly != "" {
			owner := "story_id"
			if c.row.Table == "article_locations" {
				owner = "article_id"
			}
			if _, err := tx.Exec(ctx, `UPDATE public.`+c.row.Table+` SET country=$1 WHERE `+owner+`=$2 AND place_id=$3 AND role=$4 AND country=''`, c.countryOnly, c.row.Owner, c.row.PlaceID, c.row.Role); err != nil {
				return report{}, err
			}
			continue
		}
		if err := deleteLocation(ctx, tx, c.row); err != nil {
			return report{}, err
		}
	}
	// Keep one dot per place per owner. If a replacement place already remains
	// under another role, drop the replaced row instead of adding a duplicate.
	deleted := map[ownerKey]map[string]bool{}
	for _, c := range changes {
		if c.countryOnly != "" {
			continue
		}
		k := key(c.row)
		if deleted[k] == nil {
			deleted[k] = map[string]bool{}
		}
		deleted[k][voteKey(c.row.PlaceID, c.row.Role)] = true
	}
	present := map[ownerKey]map[string]bool{}
	for _, row := range locations {
		k := key(row)
		if deleted[k][voteKey(row.PlaceID, row.Role)] {
			continue
		}
		if present[k] == nil {
			present[k] = map[string]bool{}
		}
		present[k][row.PlaceID] = true
	}
	for _, c := range changes {
		if c.countryOnly != "" {
			continue
		}
		p := c.decision.Replacement
		if p == nil {
			continue
		}
		k := key(c.row)
		if present[k] == nil {
			present[k] = map[string]bool{}
		}
		if present[k][p.ID] {
			owner, weight := "story_id", "weight"
			if c.row.Table == "article_locations" {
				owner, weight = "article_id", "conf"
			}
			// Preserve the strongest vote when replacements collide in the
			// same role. Different roles still collapse as before.
			// A retained collision target may predate migration 0006; fill an
			// unknown country from the resolved replacement without overwriting
			// an existing provenance value.
			if _, err := tx.Exec(ctx, `UPDATE public.`+c.row.Table+` SET country=$1 WHERE `+owner+`=$2 AND place_id=$3 AND country=''`, p.CountryCode, c.row.Owner, p.ID); err != nil {
				return report{}, err
			}
			if _, err := tx.Exec(ctx, `UPDATE public.`+c.row.Table+` SET `+weight+`=GREATEST(`+weight+`,$1) WHERE `+owner+`=$2 AND place_id=$3 AND role=$4`, c.row.Weight, c.row.Owner, p.ID, c.row.Role); err != nil {
				return report{}, err
			}
			continue
		}
		if err := applyLocation(ctx, tx, c.row, c.decision); err != nil {
			return report{}, err
		}
		present[k][p.ID] = true
	}
	ids := make([]string, 0, len(bundle.Stories))
	for _, p := range bundle.Stories {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, err := tx.Exec(ctx, `UPDATE public.stories s SET primary_loc=COALESCE((SELECT place_id FROM public.story_locations WHERE story_id=s.id ORDER BY (place_id=s.primary_loc) DESC,weight DESC,role,place_id LIMIT 1),'') WHERE s.id=$1`, id); err != nil {
			return report{}, err
		}
	}
	// Verify the planned complete after arrays against transaction-visible SQL,
	// including collision coalescing and primary selection, before committing.
	if err := verifyBundle(ctx, tx, bundle); err != nil {
		return report{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return report{}, fmt.Errorf("commit not confirmed for bundle %s; inspect database before retry (no committed audit emitted): %w", r.Bundle.RunID, err)
	}
	r.Status = "committed"
	r.Committed = true
	return r, nil
}

// A reviewed keep can describe the surrounding jurisdiction rather than the
// named city point. Cap its precision at the explicit title's container level.
func titleContainerPrecision(row location) (geo.Resolution, bool) {
	name := strings.ToLower(strings.TrimSpace(row.Name))
	if name == "" {
		return 0, false
	}
	for _, e := range row.Evidence {
		title := " " + strings.ToLower(e.Title) + " "
		for _, suffix := range []string{" region", " state", " province"} {
			if strings.Contains(title, " "+name+suffix) {
				return geo.RegionResolution, true
			}
		}
		if strings.Contains(title, " "+name+" district") {
			return geo.RegionResolution, true // The wire format has no district value.
		}
	}
	return 0, false
}

func loadLocations(ctx context.Context, tx pgx.Tx) ([]location, error) {
	var out []location
	// The two table/column choices are constants, never user-supplied SQL.
	for _, table := range []string{"story_locations", "article_locations"} {
		owner, weight := "story_id", "weight"
		if table == "article_locations" {
			owner, weight = "article_id", "conf"
		}
		rows, err := tx.Query(ctx, `SELECT `+owner+`,place_id,name,country,lat,lon,precision,role,`+weight+` FROM public.`+table+` ORDER BY `+owner+`,place_id,role`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			r := location{Table: table}
			if err := rows.Scan(&r.Owner, &r.PlaceID, &r.Name, &r.Country, &r.Lat, &r.Lon, &r.Precision, &r.Role, &r.Weight); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	// Load evidence in bulk, including every article attached to each story.
	byArticle, byStory := map[string][]geo.TextEvidence{}, map[string][]geo.TextEvidence{}
	rows, err := tx.Query(ctx, `SELECT id,COALESCE(story_id,''),title,lede,dateline FROM public.articles ORDER BY id,created_at`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var article, story string
		var e geo.TextEvidence
		if err := rows.Scan(&article, &story, &e.Title, &e.Lede, &e.Dateline); err != nil {
			rows.Close()
			return nil, err
		}
		byArticle[article] = append(byArticle[article], e)
		if story != "" {
			byStory[story] = append(byStory[story], e)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, `SELECT id,title FROM public.stories ORDER BY id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, title string
		if err := rows.Scan(&id, &title); err != nil {
			rows.Close()
			return nil, err
		}
		byStory[id] = append(byStory[id], geo.TextEvidence{Title: title})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Table == "story_locations" {
			out[i].Evidence = byStory[out[i].Owner]
		} else {
			out[i].Evidence = byArticle[out[i].Owner]
		}
	}
	return out, nil
}

func applyLocation(ctx context.Context, tx pgx.Tx, row location, d geo.RepairDecision) error {
	owner, weight := "story_id", "weight"
	if row.Table == "article_locations" {
		owner, weight = "article_id", "conf"
	}

	if d.Replacement == nil {
		return nil
	}
	p := d.Replacement
	name := p.Name
	if strings.TrimSpace(name) == "" {
		name = row.Name
	}
	_, err := tx.Exec(ctx, `INSERT INTO public.`+row.Table+` (`+owner+`,place_id,name,country,lat,lon,precision,role,`+weight+`)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (`+owner+`,place_id,role) DO UPDATE
 SET name=EXCLUDED.name,country=EXCLUDED.country,lat=EXCLUDED.lat,lon=EXCLUDED.lon,precision=EXCLUDED.precision,
 `+weight+`=GREATEST(`+row.Table+`.`+weight+`,EXCLUDED.`+weight+`)`, row.Owner, p.ID, name, replacementCountry(row, p), p.Lat, p.Lon, int16(d.Precision), row.Role, row.Weight)
	return err
}

// Preserve a nonempty stored country on refresh; fill an unknown country from
// the known place ID. A different place uses its own resolved country.
func replacementCountry(row location, p *geo.Place) string {
	if p.ID == row.PlaceID && (row.Country != "" || p.CountryCode == "") {
		return row.Country
	}
	return p.CountryCode
}

func deleteLocation(ctx context.Context, tx pgx.Tx, row location) error {
	owner := "story_id"
	if row.Table == "article_locations" {
		owner = "article_id"
	}
	_, err := tx.Exec(ctx, `DELETE FROM public.`+row.Table+` WHERE `+owner+`=$1 AND place_id=$2 AND role=$3`, row.Owner, row.PlaceID, row.Role)
	return err
}
