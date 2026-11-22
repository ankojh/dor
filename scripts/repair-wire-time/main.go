// Command repair-wire-time audits retained future publication times and prepares
// candidate fetch-time repairs. It never changes NATS or the live database.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ankojh/dor/internal/agents"
	"github.com/ankojh/dor/internal/bus"
	dorv1 "github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
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
	Database    string       `json:"database"`
	AsOf        time.Time    `json:"as_of"`
	IngestFirst uint64       `json:"ingest_first"`
	IngestLast  uint64       `json:"ingest_last"`
	Changes     []*candidate `json:"changes"`
	Committed   bool         `json:"committed"`
	Warning     string       `json:"warning"`
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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dbURL := flag.String("database", os.Getenv("DOR_DATABASE_URL"), "Postgres URL")
	natsURL := flag.String("nats", "nats://127.0.0.1:4222", "NATS URL (read-only direct message fetches)")
	apply := flag.Bool("apply", false, "apply candidate changes to an explicitly named copy only")
	allowDB := flag.String("allow-db", "", "exact copy database name; dor is forbidden")
	confirm := flag.Bool("confirm-candidates", false, "operator has verified each retained raw fetch time is the original")
	flag.Parse()
	if *dbURL == "" {
		return errors.New("database URL required")
	}
	ctx := context.Background()
	db, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	var name string
	var asOf time.Time
	if err = db.QueryRow(ctx, "SELECT current_database(), statement_timestamp()").Scan(&name, &asOf); err != nil {
		return err
	}
	if *apply && (*allowDB == "" || *allowDB != name || name == "dor" || !*confirm) {
		return errors.New("apply requires --allow-db matching a non-dor copy and --confirm-candidates")
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, "SELECT id, published_at, created_at, coalesce(story_id,'') FROM articles WHERE published_at > $1 ORDER BY id, created_at", asOf)
	if err != nil {
		return err
	}
	var changes []*candidate
	byID := map[string][]*candidate{}
	for rows.Next() {
		c := &candidate{}
		if err = rows.Scan(&c.ID, &c.Published, &c.Created, &c.StoryID); err != nil {
			return err
		}
		changes = append(changes, c)
		byID[c.ID] = append(byID[c.ID], c)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	nc, err := nats.Connect(*natsURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, bus.StreamIngest)
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && seq > 0; seq++ {
		msg, e := stream.GetMsg(ctx, seq)
		if errors.Is(e, jetstream.ErrMsgNotFound) {
			continue
		}
		if e != nil {
			return e
		}
		var raw dorv1.RawArticle
		if e = proto.Unmarshal(msg.Data, &raw); e != nil {
			return fmt.Errorf("raw sequence %d: %w", seq, e)
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
	out := report{Database: name, AsOf: asOf, IngestFirst: info.State.FirstSeq, IngestLast: info.State.LastSeq, Changes: changes, Warning: "Raw retention is limited; later refetch is not necessarily the original. Publisher snapshots, journal and derived state require a separate fenced reconciliation; no live cutover is authorized."}
	if *apply {
		tx, err = db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, "LOCK TABLE articles IN SHARE ROW EXCLUSIVE MODE"); err != nil {
			return err
		}
		for _, c := range changes {
			if c.RepairAt == 0 {
				// Preserve the article and raw evidence; suppress its presentation.
				tag, e := tx.Exec(ctx, `INSERT INTO wire_time_suppression (article_id, observed_published_at, reason, audited_at)
SELECT id, published_at, $3, $4 FROM articles WHERE id=$1 AND published_at=$2 AND created_at=$5
ON CONFLICT (article_id) DO UPDATE SET observed_published_at=EXCLUDED.observed_published_at, reason=EXCLUDED.reason, audited_at=EXCLUDED.audited_at`, c.ID, c.Published, c.Reason, asOf, c.Created)
				if e != nil {
					return e
				}
				if tag.RowsAffected() != 1 {
					return errors.New("article changed since audit; transaction aborted")
				}
				continue
			}
			t := time.Unix(c.RepairAt, 0).UTC()
			if _, err = tx.Exec(ctx, "SELECT ensure_article_partition($1::date)", t.Format("2006-01-02")); err != nil {
				return err
			}
			tag, e := tx.Exec(ctx, "UPDATE articles SET published_at=$1, created_at=$1 WHERE id=$2 AND published_at=$3 AND created_at=$4", t, c.ID, c.Published, c.Created)
			if e != nil {
				return e
			}
			if tag.RowsAffected() != 1 {
				return errors.New("article changed since audit; transaction aborted")
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		out.Committed = true
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
