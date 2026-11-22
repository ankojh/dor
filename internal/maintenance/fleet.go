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
	Version int    `json:"version"`
	ID      string `json:"id"`
	Epoch   uint64 `json:"epoch"`
	Serving bool   `json:"serving"`
	FloorT  uint64 `json:"floor_t"`
	FloorD  uint64 `json:"floor_d"`
	At      int64  `json:"at"`
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
	lister, err := kv.ListKeysFiltered(ctx, KeyGatewayPrefix+"*")
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

// AwaitGatewayExclusion waits until every live floor-aware gateway reports
// serving=false, or its heartbeat expires. A process that never advertises a
// heartbeat is outside this mechanism and must be excluded by the external
// fleet routing/quiesce barrier.
func AwaitGatewayExclusion(ctx context.Context, kv jetstream.KeyValue, timeout time.Duration) error {
	if kv == nil {
		return errors.New("maintenance: gateway lease bucket unavailable")
	}
	deadline := time.Now().Add(timeout)
	for {
		leases, err := ActiveGatewayLeases(ctx, kv)
		if err != nil {
			return err
		}
		live := false
		for _, lease := range leases {
			if lease.Serving {
				live = true
				break
			}
		}
		if !live {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("maintenance: gateways still report serving after fence")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
