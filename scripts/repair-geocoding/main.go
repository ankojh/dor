// Command repair-geocoding audits and repairs persisted F1·1 geocoding errors.
// Dry-run is the default; --apply commits one transaction. Stop old plot/cluster
// writers before applying on the live DB, then restart with the fixed binary so
// old in-memory/KV locations cannot overwrite the repaired durable state.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/ankojh/dor/internal/geo"
	"github.com/jackc/pgx/v5"
)

type location struct {
	Table, Owner, PlaceID, Name string
	Lat, Lon                    float64
	Precision, Role             int16
	Weight                      float32
	Evidence                    []geo.TextEvidence
}

type counts struct {
	Scanned    int            `json:"scanned"`
	Flagged    int            `json:"flagged"`
	Replotted  int            `json:"replotted"`
	Downgraded int            `json:"downgraded"`
	Removed    int            `json:"removed"`
	Reasons    map[string]int `json:"reasons"`
}

func main() {
	dsn := flag.String("dsn", os.Getenv("DOR_DATABASE_URL"), "Postgres DSN (or DOR_DATABASE_URL)")
	dir := flag.String("geonames-dir", geo.DefaultDir, "existing GeoNames cache directory")
	apply := flag.Bool("apply", false, "commit repairs (default is read-only dry run)")
	dry := flag.Bool("dry-run", true, "audit without writes; --apply overrides this flag")
	reportPath := flag.String("report", "", "optional JSON audit file with each changed row")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "--dsn or DOR_DATABASE_URL is required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := runReport(ctx, *dsn, *dir, *apply || !*dry, *reportPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn, dir string, apply bool) error {
	return runReport(ctx, dsn, dir, apply, "")
}

func runReport(ctx context.Context, dsn, dir string, apply bool, reportPath string) error {
	var report io.Writer = io.Discard
	if reportPath != "" {
		file, err := os.Create(reportPath)
		if err != nil {
			return err
		}
		defer file.Close()
		report = file
	}
	g, err := geo.Load(ctx, dir, slog.Default())
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	mode := pgx.ReadOnly
	if apply {
		mode = pgx.ReadWrite
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: mode})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if apply {
		// Prevent a running durable writer from changing rows mid-repair.
		if _, err := tx.Exec(ctx, `LOCK TABLE stories, story_locations, article_locations IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return err
		}
	}
	locations, err := loadLocations(ctx, tx)
	if err != nil {
		return err
	}
	totals := map[string]*counts{}
	for _, table := range []string{"story_locations", "article_locations"} {
		totals[table] = &counts{Reasons: map[string]int{}}
	}
	changedStories := map[string]bool{}
	type change struct {
		row      location
		decision geo.RepairDecision
	}
	var changes []change
	for _, row := range locations {
		c := totals[row.Table]
		c.Scanned++
		d := g.RepairLocation(geo.StoredLocation{PlaceID: row.PlaceID, Lat: row.Lat, Lon: row.Lon, Precision: geo.Resolution(row.Precision)}, row.Evidence)
		if d.Reason == "" {
			continue
		}
		if err := json.NewEncoder(report).Encode(struct {
			Table       string         `json:"table"`
			Owner       string         `json:"owner"`
			OldPlace    string         `json:"old_place"`
			OldName     string         `json:"old_name"`
			Reason      string         `json:"reason"`
			Replacement *geo.Place     `json:"replacement"`
			Precision   geo.Resolution `json:"precision"`
		}{row.Table, row.Owner, row.PlaceID, row.Name, d.Reason, d.Replacement, d.Precision}); err != nil {
			return err
		}
		c.Flagged++
		c.Reasons[d.Reason]++
		switch {
		case d.Replacement == nil:
			c.Removed++
		case d.Reason == "precision":
			c.Downgraded++
		default:
			c.Replotted++
		}
		if !apply {
			continue
		}
		changes = append(changes, change{row, d})
		if row.Table == "story_locations" {
			changedStories[row.Owner] = true
		}
	}
	// Delete every rejected row before inserting replacements. A replacement
	// may share a primary key with another rejected row from this snapshot.
	for _, change := range changes {
		if err := deleteLocation(ctx, tx, change.row); err != nil {
			return err
		}
	}
	for _, change := range changes {
		if err := applyLocation(ctx, tx, change.row, change.decision); err != nil {
			return err
		}
	}
	for id := range changedStories {
		if _, err := tx.Exec(ctx, `UPDATE stories s SET primary_loc=COALESCE(
 (SELECT place_id FROM story_locations WHERE story_id=s.id
  ORDER BY (place_id=s.primary_loc) DESC, weight DESC, role, place_id LIMIT 1), '') WHERE s.id=$1`, id); err != nil {
			return err
		}
	}
	if apply {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		DryRun bool               `json:"dry_run"`
		Tables map[string]*counts `json:"tables"`
	}{!apply, totals})
}

func loadLocations(ctx context.Context, tx pgx.Tx) ([]location, error) {
	var out []location
	// The two table/column choices are constants, never user-supplied SQL.
	for _, table := range []string{"story_locations", "article_locations"} {
		owner, weight := "story_id", "weight"
		if table == "article_locations" {
			owner, weight = "article_id", "conf"
		}
		rows, err := tx.Query(ctx, `SELECT `+owner+`,place_id,name,lat,lon,precision,role,`+weight+` FROM `+table+` ORDER BY `+owner+`,place_id,role`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			r := location{Table: table}
			if err := rows.Scan(&r.Owner, &r.PlaceID, &r.Name, &r.Lat, &r.Lon, &r.Precision, &r.Role, &r.Weight); err != nil {
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
	rows, err := tx.Query(ctx, `SELECT id,COALESCE(story_id,''),title,lede,dateline FROM articles ORDER BY id,created_at`)
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
	rows, err = tx.Query(ctx, `SELECT id,title FROM stories ORDER BY id`)
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
	_, err := tx.Exec(ctx, `INSERT INTO `+row.Table+` (`+owner+`,place_id,name,lat,lon,precision,role,`+weight+`)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (`+owner+`,place_id,role) DO UPDATE
 SET name=EXCLUDED.name,lat=EXCLUDED.lat,lon=EXCLUDED.lon,precision=EXCLUDED.precision,
 `+weight+`=GREATEST(`+row.Table+`.`+weight+`,EXCLUDED.`+weight+`)`, row.Owner, p.ID, name, p.Lat, p.Lon, int16(d.Precision), row.Role, row.Weight)
	return err
}

func deleteLocation(ctx context.Context, tx pgx.Tx, row location) error {
	owner := "story_id"
	if row.Table == "article_locations" {
		owner = "article_id"
	}
	_, err := tx.Exec(ctx, `DELETE FROM `+row.Table+` WHERE `+owner+`=$1 AND place_id=$2 AND role=$3`, row.Owner, row.PlaceID, row.Role)
	return err
}
