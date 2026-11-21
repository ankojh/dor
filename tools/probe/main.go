// Command probe is a small development helper for the echo acceptance test:
// it publishes numbered messages and counts what came back.
//
// Usage:
//
//	probe publish --subject echo.in --count 100 --prefix smoke --rate 200
//	probe collect --subject echo.out --expected 100 --idle 3s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ankojh/dor/internal/bus"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("missing command: publish or collect")
	}
	switch args[0] {
	case "publish":
		return publish(args[1:])
	case "collect":
		return collect(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func connect(ctx context.Context, url string) (*bus.Bus, error) {
	return bus.Connect(ctx, bus.Options{URL: url})
}

func publish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	url := fs.String("url", "nats://127.0.0.1:4222", "NATS URL")
	subject := fs.String("subject", bus.SubjectEchoIn, "subject to publish to")
	count := fs.Int("count", 100, "number of messages")
	prefix := fs.String("prefix", "probe", "message id and body prefix")
	rate := fs.Int("rate", 0, "messages per second; 0 = as fast as possible")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *count <= 0 {
		return errors.New("count must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	b, err := connect(ctx, *url)
	if err != nil {
		return err
	}
	defer b.Close()

	var interval time.Duration
	if *rate > 0 {
		interval = time.Second / time.Duration(*rate)
	}
	start := time.Now()
	for i := 0; i < *count; i++ {
		id := fmt.Sprintf("%s-%d", *prefix, i)
		body := []byte(fmt.Sprintf("%s-%d", *prefix, i))
		headers := nats.Header{}
		headers.Set(bus.HeaderArticleID, "article-"+id)
		if _, err := b.Publish(ctx, *subject, body, bus.WithMsgID(id), bus.WithHeaders(headers)); err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
		if interval > 0 && i < *count-1 {
			time.Sleep(interval)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"published": *count,
		"prefix":    *prefix,
		"subject":   *subject,
		"duration":  time.Since(start).String(),
	})
}

type collectResult struct {
	Subject      string `json:"subject"`
	Expected     int    `json:"expected"`
	Total        int    `json:"total"`
	UniqueBodies int    `json:"unique_bodies"`
	UniqueIDs    int    `json:"unique_ids"`
	Duplicates   int    `json:"duplicates"`
	Missing      int    `json:"missing"`
	Elapsed      string `json:"elapsed"`
}

func collect(args []string) error {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	url := fs.String("url", "nats://127.0.0.1:4222", "NATS URL")
	subject := fs.String("subject", bus.SubjectEchoOut, "subject to count")
	expected := fs.Int("expected", 100, "expected number of messages")
	timeout := fs.Duration("timeout", 90*time.Second, "overall timeout")
	idle := fs.Duration("idle", 3*time.Second, "stop after this much quiet once expected is reached")
	bodyPrefix := fs.String("body-prefix", "", "optional required prefix for every body")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	b, err := connect(ctx, *url)
	if err != nil {
		return err
	}
	defer b.Close()

	js := b.JS()
	streamName := bus.StreamForSubject(*subject)
	if streamName == "" {
		return fmt.Errorf("subject %q belongs to no stream", *subject)
	}
	consumer, err := js.CreateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		FilterSubject:     *subject,
		AckPolicy:         jetstream.AckExplicitPolicy,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxAckPending:     10000,
		InactiveThreshold: time.Minute,
	})
	if err != nil {
		return fmt.Errorf("create collector consumer: %w", err)
	}

	var mu sync.Mutex
	bodies := make(map[string]struct{})
	ids := make(map[string]struct{})
	total := 0
	last := time.Now()
	badPrefix := 0

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		mu.Lock()
		total++
		bodies[string(msg.Data())] = struct{}{}
		ids[msg.Headers().Get(bus.HeaderMsgID)] = struct{}{}
		if *bodyPrefix != "" && !strings.HasPrefix(string(msg.Data()), *bodyPrefix) {
			badPrefix++
		}
		last = time.Now()
		mu.Unlock()
		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", *subject, err)
	}
	defer cc.Stop()

	start := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s with %d/%d messages", *timeout, snapshotTotal(&mu, &total), *expected)
		case <-ticker.C:
			mu.Lock()
			n := total
			quiet := time.Since(last)
			mu.Unlock()
			if n >= *expected && quiet >= *idle {
				mu.Lock()
				res := collectResult{
					Subject:      *subject,
					Expected:     *expected,
					Total:        total,
					UniqueBodies: len(bodies),
					UniqueIDs:    len(ids),
					Elapsed:      time.Since(start).String(),
				}
				if total > len(bodies) {
					res.Duplicates = total - len(bodies)
				}
				if len(bodies) < *expected {
					res.Missing = *expected - len(bodies)
				}
				bad := badPrefix
				mu.Unlock()
				_ = json.NewEncoder(os.Stdout).Encode(res)
				if res.Total != *expected || res.UniqueBodies != *expected || res.UniqueIDs != *expected {
					return fmt.Errorf("echo.out mismatch: total=%d unique_bodies=%d unique_ids=%d expected=%d duplicates=%d missing=%d",
						res.Total, res.UniqueBodies, res.UniqueIDs, *expected, res.Duplicates, res.Missing)
				}
				if bad > 0 {
					return fmt.Errorf("%d messages did not start with %q", bad, *bodyPrefix)
				}
				return nil
			}
		}
	}
}

func snapshotTotal(mu *sync.Mutex, total *int) int {
	mu.Lock()
	defer mu.Unlock()
	return *total
}
