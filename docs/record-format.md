# Record format

Record mode captures the raw fetch stream so a day can be replayed later
(M6 replayer, plan §14). It is enabled with `--record-dir DIR` or
`DOR_RECORD_DIR=DIR`; it is off by default.

```
DIR/
  2026-11-20/
    07.pb.gz        first segment of hour 07
    07-2.pb.gz      second segment of hour 07 (process restarted in that hour)
    08.pb.gz
  2026-11-21/
    00.pb.gz
```

## What is recorded

Every message a fetcher publishes to `article.raw` or `article.raw.priority`,
after the PubAck, is handed to the recorder. Payloads are the serialized
`dor.v1.RawArticle` bytes exactly as published, so a replay needs no fetcher,
no network and no adapter.

The recorder never blocks a fetcher. Fetchers call `Writer.Record`, which stamps
`received_at`, copies the frame into a bounded channel (4096 by default) and
returns immediately. If the queue is full the frame is dropped and counted in
`dor_record_dropped_total`; a disk error drops that frame and increments
`dor_record_write_errors_total`; the raw stream itself is unaffected. A publish
that JetStream reports as a duplicate is not recorded at all. `Record` after
`Close` is a no-op, so a shutdown race cannot panic on a closed channel.

## File encoding

Each file is one gzip stream (one gzip member) containing:

```
offset 0   8 bytes   magic "DORREC\0\1"
offset 8   frames, repeated:
              4 bytes  big-endian uint32 N, the protobuf length
              N bytes  dor.v1.RecordFrame
```

`RecordFrame` (proto/dor/v1/dor.proto):

| Field | Type | Meaning |
| --- | --- | --- |
| `subject` | string | `article.raw` or `article.raw.priority` |
| `msg_id` | string | the `Nats-Msg-Id` used on the bus (`raw:<sha256(url)>`) |
| `received_at_ms` | int64 | unix milliseconds when the writer accepted the frame |
| `payload` | bytes | serialized `RawArticle` |

The writer flushes the gzip stream every 64 frames or every 5 seconds,
whichever comes first, so a crash loses at most the last few seconds of the
current hour.

## Rotation and restarts

- A new file starts when `received_at` crosses an hour boundary, and a new day
  directory when it crosses midnight. File names are UTC.
- On restart the writer **does not append** to an existing file: it opens the
  next free segment name (`07-2.pb.gz`, `07-3.pb.gz`, ...) with its own magic
  header. A truncated tail from a crashed process therefore never corrupts
  later data, and every segment is independently readable. Segments of one
  hour are ordered by segment number (`07`, `07-2`, `07-3`, ... `07-10`; plain
  name order would put `07-2` before `07`), which reproduces the original order.
- A crash can truncate the final frame of a segment. The reader treats an
  unexpected end of file as a clean end and returns the frames read so far;
  everything before the truncated tail is intact. There is no checksum layer
  inside a frame — a partially written final frame is simply lost.
- The reader orders one hour's files by hour and then by numeric segment
  suffix (`07.pb.gz`, `07-2.pb.gz`, `07-10.pb.gz`), which is write order.

## Reading

`internal/record.Reader` is the M6 input:

```go
r := record.NewReader(dir)
days, _ := r.Days()                    // sorted YYYY-MM-DD
files, _ := r.Files("2026-11-20")      // sorted by hour, then segment
err := r.ReadDay("2026-11-20", func(f record.Frame) error {
    // f.Subject, f.MsgID, f.ReceivedAt, f.Payload
    return nil
})
err = r.ReadAll(func(f record.Frame) error { ... })
stats, err := r.Stat("2026-11-20")     // frames, bytes, first/last, per-hour counts
```

A replay republishes on `f.Subject` with the original payload. `tools/replayer`
uses a stage-specific id
`replay:<run-id>:<raw|raw-priority>:<f.MsgID>` (not `f.MsgID` itself), so a
replay is distinguishable from live ingest and repeating the same run is
idempotent inside the JetStream dedupe window without colliding with the
recording. It also sets `Dor-Replay-Id` and `Dor-Replay-Source-Msg-Id` headers
for provenance, and paces frames by `ReceivedAt`; N× is the same loop with the
sleep divided by N. Files are already in time order. The replayer never fetches
an article body.

## Size

Frames are the same bytes the bus carries: a few hundred bytes to a couple of
kilobytes each. A quiet v1 day (a few thousand RSS items plus GDELT's capped
300 per 15-minute file) compresses to well under a gigabyte per day; gzip level
1 (`BestSpeed`) keeps the writer cheap. The bus retention is 6 h for INGEST, so
record mode is the only way to keep a whole day.
