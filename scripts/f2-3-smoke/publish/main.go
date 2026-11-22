// Command f23smoke publishes one valid RawArticle into a running Dor stack.
// It exists only for the F2·3 split-process acceptance check: the normal
// fetchers need external feeds, and the smoke needs a deterministic article.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

func main() {
	url := flag.String("url", "nats://127.0.0.1:4222", "NATS URL")
	id := flag.String("id", "f23-smoke", "raw article id suffix")
	title := flag.String("title", "F2-3 smoketest wildfire evacuation in California", "article title")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{URL: *url})
	if err != nil {
		fail("connect: %v", err)
	}
	defer b.Close()

	now := time.Now().UTC().Unix()
	raw := &dorv1.RawArticle{
		Id:          "raw:" + *id,
		SourceId:    "f23-smoke",
		Kind:        dorv1.SourceKind_SOURCE_KIND_RSS,
		Url:         "https://example.com/f2-3-smoke/" + *id,
		Title:       *title,
		Summary:     *title,
		PublishedAt: now,
		FetchedAt:   now,
		Lang:        "en",
	}
	data, err := proto.Marshal(raw)
	if err != nil {
		fail("marshal: %v", err)
	}
	headers := nats.Header{}
	headers.Set(bus.HeaderArticleID, raw.Id)
	if _, err := b.Publish(ctx, bus.SubjectArticleRaw, data,
		bus.WithMsgID("raw:f23-smoke:"+*id), bus.WithHeaders(headers)); err != nil {
		fail("publish: %v", err)
	}
	fmt.Println(raw.Id)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "f23smoke: "+format+"\n", args...)
	os.Exit(1)
}
