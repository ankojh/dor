package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// KeyGatewayPrefix is the non-expiring per-process gateway advertisement. Keys
// are removed on clean stop and ignored after GatewayLeaseTTL so a crashed
// process does not block maintenance forever.
const KeyGatewayPrefix = "maintenance/gateway."

// GatewayLeaseTTL bounds how long a gateway heartbeat is authoritative.
const GatewayLeaseTTL = 5 * time.Second

// GatewayLease is one gateway's current serving state. Epoch is the control
// FleetEpoch observed by that process.
type GatewayLease struct {
	Version       int    `json:"version"`
	ID            string `json:"id"`
	Epoch         uint64 `json:"epoch"`
	Serving       bool   `json:"serving"`
	WireTombstone bool   `json:"wire_tombstone"`
	FloorT        uint64 `json:"floor_t"`
	FloorD        uint64 `json:"floor_d"`
	At            int64  `json:"at"`
}

// WriteGatewayLease publishes one gateway's heartbeat.
func WriteGatewayLease(ctx context.Context, kv jetstream.KeyValue, lease GatewayLease) error {
	if lease.ID == "" {
		return errors.New("maintenance: gateway lease has no id")
	}
	lease.Version = ControlVersion
	lease.At = time.Now().UnixNano()
	key := KeyGatewayPrefix + lease.ID
	data, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 8; attempt++ {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if _, err = kv.Create(ctx, key, data); err == nil {
				return nil
			}
		} else if err == nil {
			if _, err = kv.Update(ctx, key, data, entry.Revision()); err == nil {
				return nil
			}
		} else {
			return err
		}
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) && !errors.Is(err, jetstream.ErrKeyExists) {
			return err
		}
	}
	return errors.New("maintenance: gateway lease contention")
}

// DeleteGatewayLease removes a cleanly stopped gateway's heartbeat.
func DeleteGatewayLease(ctx context.Context, kv jetstream.KeyValue, id string) error {
	if kv == nil || id == "" {
		return nil
	}
	err := kv.Delete(ctx, KeyGatewayPrefix+id)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

// ActiveGatewayLeases returns heartbeats that are still within GatewayLeaseTTL.
func ActiveGatewayLeases(ctx context.Context, kv jetstream.KeyValue) ([]GatewayLease, error) {
	lister, err := kv.ListKeysFiltered(ctx, KeyGatewayPrefix+">")
	if err != nil {
		return nil, err
	}
	defer func() { _ = lister.Stop() }()
	var out []GatewayLease
	for key := range lister.Keys() {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var lease GatewayLease
		if err := json.Unmarshal(entry.Value(), &lease); err != nil {
			return nil, fmt.Errorf("maintenance: corrupt gateway lease %s: %w", key, err)
		}
		if lease.Version != ControlVersion {
			return nil, fmt.Errorf("maintenance: unsupported gateway lease version in %s", key)
		}
		if time.Since(time.Unix(0, lease.At)) > GatewayLeaseTTL {
			continue
		}
		out = append(out, lease)
	}
	return out, nil
}

// ProducerLease is one fetch/write runtime's maintenance state.
type ProducerLease struct {
	Version            int            `json:"version"`
	ID                 string         `json:"id"`
	Epoch              uint64         `json:"epoch"`
	Healthy            bool           `json:"healthy"`
	Error              string         `json:"error,omitempty"`
	Paused             bool           `json:"paused"`
	IngressClosed      bool           `json:"ingress_closed"`
	DownstreamClosed   bool           `json:"downstream_closed"`
	IngestInflight     int64          `json:"ingress_inflight"`
	DownstreamInflight int64          `json:"downstream_inflight"`
	IngestFirst        uint64         `json:"ingest_first"`
	IngestLast         uint64         `json:"ingest_last"`
	Writer             WriterCounters `json:"writer"`
	At                 int64          `json:"at"`
}

// WriteProducerLease publishes one runtime heartbeat.
func WriteProducerLease(ctx context.Context, kv jetstream.KeyValue, lease ProducerLease) error {
	if lease.ID == "" {
		return errors.New("maintenance: producer lease has no id")
	}
	lease.Version = ControlVersion
	lease.At = time.Now().UnixNano()
	data, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	key := lease.ID
	for attempt := 0; attempt < 8; attempt++ {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			if _, err = kv.Create(ctx, key, data); err == nil {
				return nil
			}
		} else if err == nil {
			if _, err = kv.Update(ctx, key, data, entry.Revision()); err == nil {
				return nil
			}
		} else {
			return err
		}
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) && !errors.Is(err, jetstream.ErrKeyExists) {
			return err
		}
	}
	return errors.New("maintenance: producer lease contention")
}

// DeleteProducerLease removes a runtime heartbeat after a failed refresh so an
// old successful lease cannot be mistaken for a current barrier.
func DeleteProducerLease(ctx context.Context, kv jetstream.KeyValue, id string) error {
	if kv == nil || id == "" {
		return nil
	}
	err := kv.Delete(ctx, id)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

// ActiveProducerLeases returns unexpired runtime heartbeats.
func ActiveProducerLeases(ctx context.Context, kv jetstream.KeyValue) ([]ProducerLease, error) {
	lister, err := kv.ListKeysFiltered(ctx, ">")
	if err != nil {
		return nil, err
	}
	defer func() { _ = lister.Stop() }()
	var out []ProducerLease
	for key := range lister.Keys() {
		entry, err := kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var lease ProducerLease
		if err := json.Unmarshal(entry.Value(), &lease); err != nil {
			return nil, fmt.Errorf("maintenance: corrupt producer lease %s: %w", key, err)
		}
		if lease.Version != ControlVersion {
			return nil, fmt.Errorf("maintenance: unsupported producer lease in %s", key)
		}
		if time.Since(time.Unix(0, lease.At)) > GatewayLeaseTTL {
			continue
		}
		out = append(out, lease)
	}
	return out, nil
}

// AwaitProducerFence requires every expected producer to report paused with no
// in-flight ingest publish and no queued/in-flight/retry DB work. Missing or
// unregistered IDs are excluded only by explicit operator evidence.
// AwaitProducerAdmission closes external/raw admission while allowing the
// finite downstream backlog to continue draining.
func AwaitProducerAdmission(ctx context.Context, kv jetstream.KeyValue, expected, excluded []string, timeout time.Duration) ([]ProducerLease, error) {
	if kv == nil {
		return nil, errors.New("maintenance: producer lease bucket unavailable")
	}
	if len(expected) == 0 {
		return nil, errors.New("maintenance: expected producer inventory is required")
	}
	excludedSet := map[string]bool{}
	for _, id := range excluded {
		excludedSet[id] = true
	}
	deadline := time.Now().Add(timeout)
	for {
		leases, err := ActiveProducerLeases(ctx, kv)
		if err != nil {
			return nil, err
		}
		byID := map[string]ProducerLease{}
		for _, lease := range leases {
			byID[lease.ID] = lease
		}
		blocked := []string{}
		for _, id := range expected {
			if excludedSet[id] {
				continue
			}
			lease, ok := byID[id]
			if !ok {
				blocked = append(blocked, id+" missing")
				continue
			}
			if !lease.Healthy {
				blocked = append(blocked, id+" barrier unhealthy: "+lease.Error)
				continue
			}
			if !lease.IngressClosed || lease.IngestInflight > 0 {
				blocked = append(blocked, id+" ingress not closed")
			}
		}
		if len(blocked) == 0 {
			return leases, nil
		}
		if time.Now().After(deadline) {
			return leases, fmt.Errorf("maintenance: ingress admission not fenced: %v", blocked)
		}
		select {
		case <-ctx.Done():
			return leases, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// AwaitProducerFence waits for downstream writers and guarded KV operations to
// close and settle after the finite drain.
func AwaitProducerFence(ctx context.Context, kv jetstream.KeyValue, expected, excluded []string, timeout time.Duration) ([]ProducerLease, error) {
	if kv == nil {
		return nil, errors.New("maintenance: producer lease bucket unavailable")
	}
	if len(expected) == 0 {
		return nil, errors.New("maintenance: expected producer inventory is required")
	}
	excludedSet := map[string]bool{}
	for _, id := range excluded {
		excludedSet[id] = true
	}
	deadline := time.Now().Add(timeout)
	for {
		leases, err := ActiveProducerLeases(ctx, kv)
		if err != nil {
			return nil, err
		}
		byID := map[string]ProducerLease{}
		for _, lease := range leases {
			byID[lease.ID] = lease
		}
		blocked := []string{}
		for _, id := range expected {
			if excludedSet[id] {
				continue
			}
			lease, ok := byID[id]
			if !ok {
				blocked = append(blocked, id+" missing")
				continue
			}
			if !lease.Healthy {
				blocked = append(blocked, id+" barrier unhealthy: "+lease.Error)
				continue
			}
			if !lease.IngressClosed || lease.IngestInflight > 0 {
				blocked = append(blocked, id+" ingress in flight")
				continue
			}
			if !lease.DownstreamClosed || lease.DownstreamInflight > 0 {
				blocked = append(blocked, id+" downstream in flight")
				continue
			}
			if !lease.Writer.Verified {
				blocked = append(blocked, id+" writer barrier unavailable")
				continue
			}
			if writerWork(lease.Writer) > 0 {
				blocked = append(blocked, id+" writer not settled")
			}
		}
		if len(blocked) == 0 {
			return leases, nil
		}
		if time.Now().After(deadline) {
			return leases, fmt.Errorf("maintenance: producers not fenced: %v", blocked)
		}
		select {
		case <-ctx.Done():
			return leases, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func writerWork(w WriterCounters) int64 {
	var n int64
	if w.Pending > 0 || w.PendingStories > 0 || w.AuxQueue > 0 {
		n++
	}
	n += w.Unsettled
	n += w.PendingEnrich
	if w.PendingAssign > 0 {
		n++
	}
	n += w.Flushing
	// Errors is cumulative diagnostics: a transient error recovered by retry
	// leaves it non-zero with no unresolved work. Only terminal drops keep the
	// barrier blocked.
	if w.DroppedQueue > 0 || w.DroppedBatch > 0 || w.DroppedStories > 0 {
		n++
	}
	return n
}

// AwaitProducerWriterClosed requires the final DB writer admission closure in
// addition to the downstream/in-flight barrier.
func AwaitProducerWriterClosed(ctx context.Context, kv jetstream.KeyValue, expected, excluded []string, timeout time.Duration) ([]ProducerLease, error) {
	if kv == nil {
		return nil, errors.New("maintenance: producer lease bucket unavailable")
	}
	if len(expected) == 0 {
		return nil, errors.New("maintenance: expected producer inventory is required")
	}
	excludedSet := map[string]bool{}
	for _, id := range excluded {
		excludedSet[id] = true
	}
	deadline := time.Now().Add(timeout)
	for {
		leases, err := ActiveProducerLeases(ctx, kv)
		if err != nil {
			return nil, err
		}
		byID := map[string]ProducerLease{}
		for _, lease := range leases {
			byID[lease.ID] = lease
		}
		blocked := []string{}
		for _, id := range expected {
			if excludedSet[id] {
				continue
			}
			lease, ok := byID[id]
			if !ok {
				blocked = append(blocked, id+" missing")
				continue
			}
			if !lease.Healthy {
				blocked = append(blocked, id+" barrier unhealthy: "+lease.Error)
				continue
			}
			if !lease.Writer.Blocked {
				blocked = append(blocked, id+" writer admission still open")
				continue
			}
			if !lease.IngressClosed || !lease.DownstreamClosed || lease.IngestInflight > 0 || lease.DownstreamInflight > 0 {
				blocked = append(blocked, id+" in-flight operation remains")
				continue
			}
			if !lease.Writer.Verified || writerWork(lease.Writer) > 0 {
				blocked = append(blocked, id+" writer work remains")
			}
		}
		if len(blocked) == 0 {
			return leases, nil
		}
		if time.Now().After(deadline) {
			return leases, fmt.Errorf("maintenance: writer admission not closed: %v", blocked)
		}
		select {
		case <-ctx.Done():
			return leases, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// AwaitGatewayExclusion waits until every expected floor-aware gateway reports
// serving=false with wire-tombstone compatibility. Missing/incompatible IDs are
// excluded only by explicit operator evidence.
func AwaitGatewayExclusion(ctx context.Context, kv jetstream.KeyValue, expected, excluded []string, timeout time.Duration) error {
	if kv == nil {
		return errors.New("maintenance: gateway lease bucket unavailable")
	}
	if len(expected) == 0 {
		return errors.New("maintenance: expected gateway inventory is required")
	}
	excludedSet := map[string]bool{}
	for _, id := range excluded {
		excludedSet[id] = true
	}
	deadline := time.Now().Add(timeout)
	for {
		leases, err := ActiveGatewayLeases(ctx, kv)
		if err != nil {
			return err
		}
		byID := map[string]GatewayLease{}
		for _, lease := range leases {
			byID[lease.ID] = lease
		}
		blocked := []string{}
		for _, id := range expected {
			if excludedSet[id] {
				continue
			}
			lease, ok := byID[id]
			if !ok {
				if !excludedSet[id] {
					blocked = append(blocked, id+" missing")
				}
				continue
			}
			if !lease.WireTombstone {
				blocked = append(blocked, id+" is not wire-tombstone compatible")
				continue
			}
			if lease.Serving {
				blocked = append(blocked, id+" still serving")
			}
		}
		if len(blocked) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("maintenance: gateways not excluded: %v", blocked)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
