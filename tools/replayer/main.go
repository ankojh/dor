// Command replayer plays a recorded day (or a list of days) into a running
// local stack at N× wall-clock speed. It is the M6 performance input: record
// raw articles once with `dor run --record-dir`, then replay them.
//
// The replayer publishes to the original raw subjects only
// (article.raw and article.raw.priority) with the original payload. It never
// synthesises article bodies. Every message gets a stage-specific replay id
// that preserves the recorded producer id, plus provenance headers, so a
// replay can be distinguished from live ingest and replayed idempotently.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/record"
)

type stats struct {
	Frames      int
	Published   int
	Duplicates  int
	Bytes       int64
	Skipped     int
	AddedDays   int
	RecordedFor time.Duration
	Elapsed     time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "replayer:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	var (
		dir     = flag.String("dir", "", "record root produced by --record-dir (required)")
		days    = flag.String("day", "", "day (YYYY-MM-DD) or comma-separated days; empty selects the latest")
		rate    = flag.Float64("rate", 10, "playback speed N×; 0 means unthrottled")
		natsURL = flag.String("nats-url", envOr("DOR_NATS_URL", "nats://127.0.0.1:4222"), "existing NATS URL; the replayer never starts an embedded server")
		runID   = flag.String("run-id", "", "replay id; default is a UTC timestamp")
		dryRun  = flag.Bool("dry-run", false, "read and pace without publishing")
		limit   = flag.Int("limit", 0, "stop after N replayed frames (0 = all)")
		quiet   = flag.Bool("quiet", false, "only print the final summary")
	)
	flag.Parse()
	if *dir == "" {
		return errors.New("-dir is required")
	}
	if *rate < 0 {
		return errors.New("-rate must be >= 0")
	}
	if strings.TrimSpace(*natsURL) == "" {
		*natsURL = "nats://127.0.0.1:4222"
	}
	id := strings.TrimSpace(*runID)
	if id == "" {
		id = time.Now().UTC().Format("20060102T150405Z")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := record.NewReader(*dir)
	known, err := reader.Days()
	if err != nil {
		return err
	}
	if len(known) == 0 {
		return fmt.Errorf("no recorded days under %s", *dir)
	}
	selected, err := selectDays(*days, known)
	if err != nil {
		return err
	}

	var nc *nats.Conn
	var js jetstream.JetStream
	if !*dryRun {
		nc, err = nats.Connect(*natsURL,
			nats.Name("dor-replayer"),
			nats.Timeout(5*time.Second),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(500*time.Millisecond),
		)
		if err != nil {
			return fmt.Errorf("connect %s: %w", *natsURL, err)
		}
		defer nc.Close()
		js, err = jetstream.New(nc)
		if err != nil {
			return fmt.Errorf("jetstream: %w", err)
		}
	}

	subjects := map[string]bool{
		bus.SubjectArticleRaw:         true,
		bus.SubjectArticleRawPriority: true,
	}
	st := stats{}
	start := time.Now()
	for _, day := range selected {
		if err := replayDay(ctx, reader, day, *rate, *dryRun, *limit, id, subjects, js, *quiet, &st); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
		if *limit > 0 && st.Published >= *limit {
			break
		}
	}
	st.Elapsed = time.Since(start)
	st.AddedDays = len(selected)
	printSummary(st, *dryRun, *rate, id)
	return nil
}

func selectDays(spec string, known []string) ([]string, error) {
	available := make(map[string]bool, len(known))
	for _, day := range known {
		available[day] = true
	}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return []string{known[len(known)-1]}, nil
	}
	var out []string
	seen := make(map[string]bool)
	for _, raw := range strings.Split(spec, ",") {
		day := strings.TrimSpace(raw)
		if day == "" || seen[day] {
			continue
		}
		if !available[day] {
			return nil, fmt.Errorf("day %q is not recorded (available: %s)", day, strings.Join(known, ", "))
		}
		seen[day] = true
		out = append(out, day)
	}
	if len(out) == 0 {
		return nil, errors.New("no days selected")
	}
	sort.Strings(out)
	return out, nil
}

func replayDay(ctx context.Context, reader *record.Reader, day string, rate float64, dryRun bool, limit int, runID string, subjects map[string]bool, js jetstream.JetStream, quiet bool, st *stats) error {
	var base time.Time
	var start time.Time
	var last time.Time
	lastLogged := time.Time{}

	err := reader.ReadDay(day, func(frame record.Frame) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		st.Frames++
		if !subjects[frame.Subject] {
			st.Skipped++
			return nil
		}
		if limit > 0 && st.Published >= limit {
			return context.Canceled
		}
		at := frame.ReceivedAt
		if at.IsZero() {
			at = last
		}
		if at.IsZero() {
			at = time.Now().UTC()
		}
		if base.IsZero() {
			base = at
			start = time.Now()
		}
		last = at
		st.Bytes += int64(len(frame.Payload))
		st.RecordedFor = at.Sub(base)
		if rate > 0 {
			target := start.Add(time.Duration(float64(at.Sub(base)) / rate))
			if wait := time.Until(target); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
		if dryRun {
			st.Published++
		} else {
			msgID := replayMsgID(runID, frame)
			headers := nats.Header{}
			headers.Set(bus.HeaderReplayID, runID)
			if frame.MsgID != "" {
				headers.Set(bus.HeaderReplaySourceMsgID, frame.MsgID)
			}
			ack, err := jsPublish(ctx, js, frame.Subject, frame.Payload, msgID, headers)
			if err != nil {
				return fmt.Errorf("publish %s: %w", frame.Subject, err)
			}
			if ack.Duplicate {
				st.Duplicates++
			} else {
				st.Published++
			}
		}
		if !quiet && !lastLogged.IsZero() && time.Since(lastLogged) >= 5*time.Second {
			lastLogged = time.Now()
			fmt.Printf("[replay %s] %s · frames=%d published=%d %.0f×\n",
				runID, day, st.Frames, st.Published, rate)
		} else if !quiet && lastLogged.IsZero() {
			lastLogged = time.Now()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if err == nil || errors.Is(err, context.Canceled) {
		if !quiet {
			fmt.Printf("[replay %s] %s complete · frames=%d published=%d duplicates=%d skipped=%d\n",
				runID, day, st.Frames, st.Published, st.Duplicates, st.Skipped)
		}
	}
	return nil
}

// replayMsgID is stage-specific and keeps the recorded producer id, so two
// replay runs do not collide and one repeat of the same run is idempotent
// inside the JetStream dedupe window.
func replayMsgID(runID string, frame record.Frame) string {
	source := frame.MsgID
	if source == "" {
		source = fmt.Sprintf("at-%d", frame.ReceivedAt.UnixNano())
	}
	stage := "raw"
	if frame.Subject == bus.SubjectArticleRawPriority {
		stage = "raw-priority"
	}
	return "replay:" + runID + ":" + stage + ":" + source
}

func jsPublish(ctx context.Context, js jetstream.JetStream, subject string, data []byte, msgID string, headers nats.Header) (*jetstream.PubAck, error) {
	if headers == nil {
		headers = nats.Header{}
	}
	headers.Set(bus.HeaderMsgID, msgID)
	msg := &nats.Msg{Subject: subject, Data: data, Header: headers}
	return js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
}

func printSummary(st stats, dryRun bool, rate float64, runID string) {
	mode := "published"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Printf("replayer %s · run=%s · days=%d · speed=%.1f×\n", mode, runID, st.AddedDays, rate)
	fmt.Printf("  frames read        %d\n", st.Frames)
	fmt.Printf("  frames replayed    %d (%d duplicate acks)\n", st.Published, st.Duplicates)
	fmt.Printf("  skipped subject    %d\n", st.Skipped)
	fmt.Printf("  payload bytes      %d\n", st.Bytes)
	fmt.Printf("  recorded span      %s\n", st.RecordedFor.Round(time.Millisecond))
	fmt.Printf("  wall clock         %s\n", st.Elapsed.Round(time.Millisecond))
	if st.RecordedFor > 0 && st.Elapsed > 0 {
		fmt.Printf("  effective speed    %.2f×\n", st.RecordedFor.Seconds()/st.Elapsed.Seconds())
	}
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}
