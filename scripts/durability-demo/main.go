// durability-demo exercises a dedicated Dor/NATS stack against a copied DB.
// Start an embedded carrier on separate ports first; use a fresh NATS store.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	capacityOnly := flag.Bool("capacity-only", false, "measure copied and planned storage load without modifying NATS")
	natsURL := flag.String("nats-url", "nats://127.0.0.1:4323", "dedicated provisioned NATS")
	dbURL := flag.String("db-url", "postgres://dor:dor@127.0.0.1:5433/dor_f2_1?sslmode=disable", "copied Postgres database")
	binary := flag.String("binary", ".dor/f2-1/dor", "built dor executable")
	logDir := flag.String("log-dir", ".dor/f2-1", "demo output directory")
	gatewayAddr := flag.String("gateway-addr", "127.0.0.1:8183", "dedicated gateway listener")
	adminAddr := flag.String("admin-addr", "127.0.0.1:8194", "dedicated admin listener")
	flag.Parse()
	must(os.MkdirAll(*logDir, 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	b, err := bus.Connect(ctx, bus.Options{URL: *natsURL})
	must(err)
	defer b.Close()
	db, err := pgxpool.New(ctx, *dbURL)
	must(err)
	defer db.Close()
	snap := &dorv1.Snapshot{BuiltAt: time.Now().Unix()}
	byID := map[string]*dorv1.Story{}
	rows, err := db.Query(ctx, "select id,title,category,source_count,heat,extract(epoch from first_seen)::bigint,extract(epoch from updated_at)::bigint from stories order by id")
	must(err)
	for rows.Next() {
		s := &dorv1.Story{}
		var category int32
		must(rows.Scan(&s.Id, &s.Title, &category, &s.SourceCount, &s.Heat, &s.FirstSeen, &s.UpdatedAt))
		s.Category = dorv1.Category(category)
		snap.Stories = append(snap.Stories, s)
		byID[s.Id] = s
	}
	must(rows.Err())
	rows.Close()
	rows, err = db.Query(ctx, "select story_id,place_id,name,lat,lon,precision,role,weight from story_locations order by story_id,place_id")
	must(err)
	for rows.Next() {
		var id string
		l := &dorv1.Location{}
		var precision, role int32
		must(rows.Scan(&id, &l.PlaceId, &l.Name, &l.Lat, &l.Lon, &precision, &role, &l.Weight))
		l.Precision = dorv1.Precision(precision)
		l.Role = dorv1.Role(role)
		if s := byID[id]; s != nil {
			s.Locations = append(s.Locations, l)
		}
	}
	must(rows.Err())
	rows.Close()
	rows, err = db.Query(ctx, "select a,b,type,direction,confidence,strength from links where strength>=0.3 order by a,b")
	must(err)
	for rows.Next() {
		l := &dorv1.Link{}
		var kind, dir int32
		must(rows.Scan(&l.A, &l.B, &kind, &dir, &l.Confidence, &l.Strength))
		l.Type = dorv1.LinkType(kind)
		l.Dir = dorv1.Direction(dir)
		snap.Links = append(snap.Links, l)
	}
	must(rows.Err())
	rows.Close()
	rows, err = db.Query(ctx, "select id,coalesce(story_id,''),title,source_id,lang,extract(epoch from published_at)::bigint from articles order by published_at desc limit 500")
	must(err)
	for rows.Next() {
		w := &dorv1.WireRow{}
		must(rows.Scan(&w.ArticleId, &w.StoryId, &w.Title, &w.Source, &w.Lang, &w.At))
		snap.RecentWire = append(snap.RecentWire, w)
	}
	must(rows.Err())
	rows.Close()
	if *capacityOnly {
		must(capacityReport(b, snap))
		return
	}
	checkpointKV, err := b.KV(ctx, bus.KVBucketPublisher)
	must(err)
	if _, err = checkpointKV.Get(ctx, "checkpoint"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		panic("demo requires a fresh dedicated NATS store")
	}
	kv, err := b.KV(ctx, bus.KVBucketLiveState)
	must(err)
	data, err := live.MarshalSnapshot(snap)
	must(err)
	raw, _ := proto.Marshal(snap)
	fmt.Printf("COPY_DATA raw_proto_bytes=%d stored_bytes=%d headroom_bytes=%d\n", len(raw), len(data), b.NC().MaxPayload()-int64(len(data)))
	_, err = kv.Put(ctx, bus.KVLiveStateSnapshotKey, data)
	must(err)
	fmt.Printf("COPY_SEED stories=%d links=%d wire=%d snapshot_bytes=%d\n", len(snap.Stories), len(snap.Links), len(snap.RecentWire), len(data))
	start := func() *exec.Cmd {
		log, err := os.OpenFile(filepath.Join(*logDir, "publisher.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		must(err)
		cmd := exec.Command(*binary, "run", "--agents", "publisher,gateway", "--nats-url", *natsURL, "--gateway-addr", *gatewayAddr, "--admin-addr", *adminAddr, "--supervisor=false", "--tei-url", "", "--db-url", *dbURL)
		cmd.Stdout, cmd.Stderr = log, log
		must(cmd.Start())
		log.Close()
		return cmd
	}
	waitReady := func() {
		for {
			r, e := http.Get("http://" + *gatewayAddr + "/healthz")
			if e == nil {
				r.Body.Close()
				if r.StatusCode == 200 {
					return
				}
			}
			select {
			case <-ctx.Done():
				panic(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	connect := func() (*websocket.Conn, *dorv1.Snapshot) {
		c, _, e := websocket.Dial(ctx, "ws://"+*gatewayAddr+"/ws", nil)
		must(e)
		c.SetReadLimit(8 << 20)
		d, e := proto.Marshal(&dorv1.ClientMessage{Body: &dorv1.ClientMessage_Hello{Hello: &dorv1.Hello{}}})
		must(e)
		must(c.Write(ctx, websocket.MessageBinary, d))
		_, d, e = c.Read(ctx)
		must(e)
		var m dorv1.ServerMessage
		must(proto.Unmarshal(d, &m))
		s := m.GetSnapshot()
		if s == nil {
			panic("expected snapshot")
		}
		return c, s
	}
	cmd := start()
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	waitReady()
	cold := time.Now()
	conn, initial := connect()
	defer conn.CloseNow()
	fmt.Printf("COLD_SNAPSHOT stories=%d seq=%d latency_ms=%d\n", len(initial.Stories), initial.Seq, time.Since(cold).Milliseconds())
	if len(initial.Stories) != len(snap.Stories) || len(initial.Links) != len(snap.Links) || len(initial.RecentWire) != len(snap.RecentWire) {
		panic("missing initial stories")
	}
	publish := func(id string) {
		d, e := proto.Marshal(&dorv1.Story{Id: id, Title: id, UpdatedAt: time.Now().Unix()})
		must(e)
		_, e = b.Publish(ctx, bus.StoryChangedSubject(id), d, bus.WithMsgID(id))
		must(e)
	}
	seq := uint64(0)
	readUntil := func(target uint64) {
		for seq < target {
			_, d, e := conn.Read(ctx)
			must(e)
			var m dorv1.ServerMessage
			must(proto.Unmarshal(d, &m))
			if s := m.GetSnapshot(); s != nil {
				panic("unexpected snapshot during healthy stream")
			}
			for _, delta := range m.GetDeltas().GetDeltas() {
				if delta.Seq != seq+1 {
					panic(fmt.Sprintf("gap: %d -> %d", seq, delta.Seq))
				}
				seq = delta.Seq
			}
		}
	}
	publish("durability-demo-1")
	readUntil(1)
	stream, err := b.StreamNamed(ctx, bus.StreamDelta)
	must(err)
	info, err := stream.Info(ctx)
	must(err)
	original := info.Config
	defer func() { _, _ = b.JS().UpdateStream(context.Background(), original) }()
	blocked := original
	blocked.MaxMsgs = int64(info.State.Msgs)
	blocked.Discard = jetstream.DiscardNew
	_, err = b.JS().UpdateStream(ctx, blocked)
	must(err)
	publish("durability-demo-2")
	publish("durability-demo-3")
	time.Sleep(1300 * time.Millisecond)
	e, err := kv.Get(ctx, bus.KVLiveStateSnapshotKey)
	must(err)
	var during dorv1.Snapshot
	must(live.UnmarshalSnapshot(e.Value(), &during))
	if during.Seq != 1 {
		panic("advanced failed publish watermark")
	}
	fmt.Printf("INJECTED_FAILURE snapshot_seq=%d stable=true\n", during.Seq)
	_, err = b.JS().UpdateStream(ctx, original)
	must(err)
	readUntil(3)
	fmt.Printf("RECOVERED_DELTAS seq=%d contiguous=true\n", seq)
	// Reserve another failing batch, then SIGKILL with the websocket connected.
	info, err = stream.Info(ctx)
	must(err)
	blocked.MaxMsgs = int64(info.State.Msgs)
	_, err = b.JS().UpdateStream(ctx, blocked)
	must(err)
	publish("durability-demo-4")
	time.Sleep(600 * time.Millisecond)
	must(cmd.Process.Kill())
	_ = cmd.Wait()
	restart := time.Now()
	cmd = start()
	waitReady()
	conn.CloseNow()
	conn, after := connect()
	defer conn.CloseNow()
	fmt.Printf("RESTART_SNAPSHOT stories=%d seq=%d latency_ms=%d empty=false\n", len(after.Stories), after.Seq, time.Since(restart).Milliseconds())
	if len(after.Stories) != len(snap.Stories)+3 || len(after.Links) != len(snap.Links) || len(after.RecentWire) != len(snap.RecentWire) {
		panic("restart lost committed globe")
	}
	if after.Seq != 3 {
		panic("restart wrong watermark")
	}
	_, err = b.JS().UpdateStream(ctx, original)
	must(err)
	seq = after.Seq
	readUntil(4)
	fmt.Printf("RESTART_PENDING_RECOVERED seq=%d contiguous=true\n", seq)
	must(cmd.Process.Kill())
	_ = cmd.Wait()
	fmt.Println("PASS live restart, full snapshot, pending outbox recovery, real NATS publish failures and no gaps")
}
