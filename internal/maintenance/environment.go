package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// KeyEnvironment is the immutable isolated-deployment identity stored in
	// the publisher KV bucket. Maintenance refuses an unbound or different
	// value before it fences or writes anything.
	KeyEnvironment = "maintenance/environment"
	// EnvironmentVersion is the only supported environment record version.
	EnvironmentVersion = 1
)

// Environment is the durable copy of an isolated deployment's identity. The
// same id must be bound in the database, in this publisher record and in every
// gateway/producer lease.
type Environment struct {
	Version int    `json:"version"`
	ID      string `json:"environment_id"`
}

// NormalizeEnvironmentID trims an operator-supplied id and rejects an empty
// one. An empty id is never treated as "default" or "local".
func NormalizeEnvironmentID(raw string) (string, error) {
	id := strings.TrimSpace(raw)
	if id == "" {
		return "", errors.New("maintenance: explicit environment id is required; an unbound target is refused")
	}
	return id, nil
}

// Validate checks one persisted environment record.
func (e Environment) Validate() error {
	if e.Version != EnvironmentVersion {
		return fmt.Errorf("maintenance: unsupported environment version %d", e.Version)
	}
	if _, err := NormalizeEnvironmentID(e.ID); err != nil {
		return err
	}
	return nil
}

// ReadEnvironment reads the publisher-bucket environment record. A missing key
// is reported as a zero record with revision zero; callers that need a binding
// must fail closed.
func ReadEnvironment(ctx context.Context, kv jetstream.KeyValue) (Environment, uint64, error) {
	if kv == nil {
		return Environment{}, 0, errors.New("maintenance: environment bucket unavailable")
	}
	entry, err := kv.Get(ctx, KeyEnvironment)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Environment{}, 0, nil
	}
	if err != nil {
		return Environment{}, 0, fmt.Errorf("maintenance: read environment: %w", err)
	}
	var env Environment
	if err := json.Unmarshal(entry.Value(), &env); err != nil {
		return Environment{}, 0, fmt.Errorf("maintenance: corrupt environment record: %w", err)
	}
	if err := env.Validate(); err != nil {
		return Environment{}, 0, fmt.Errorf("maintenance: corrupt environment record: %w", err)
	}
	return env, entry.Revision(), nil
}

// RequireEnvironment compares one durable identity (database row, publisher
// record or participant lease) with the explicitly requested environment.
func RequireEnvironment(name, got, want string) error {
	expected, err := NormalizeEnvironmentID(want)
	if err != nil {
		return err
	}
	got = strings.TrimSpace(got)
	if got == "" {
		return fmt.Errorf("maintenance: %s environment identity is unbound; bind it before audit/apply", name)
	}
	if got != expected {
		return fmt.Errorf("maintenance: %s belongs to environment %q, not %q", name, got, expected)
	}
	return nil
}

// PutEnvironment binds the publisher environment record exactly once. An
// existing different id is refused; an existing equal id is idempotent. It is
// only called by the explicit bind command, never by audit/apply.
func PutEnvironment(ctx context.Context, kv jetstream.KeyValue, id string) error {
	want, err := NormalizeEnvironmentID(id)
	if err != nil {
		return err
	}
	if kv == nil {
		return errors.New("maintenance: environment bucket unavailable")
	}
	current, revision, err := ReadEnvironment(ctx, kv)
	if err != nil {
		return err
	}
	if revision != 0 {
		if current.ID != want {
			return fmt.Errorf("maintenance: publisher environment is already bound to %q, not %q", current.ID, want)
		}
		return nil
	}
	data, err := json.Marshal(Environment{Version: EnvironmentVersion, ID: want})
	if err != nil {
		return err
	}
	if _, err := kv.Create(ctx, KeyEnvironment, data); err != nil {
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return fmt.Errorf("maintenance: bind environment: %w", err)
		}
		// Someone bound it concurrently; accept only the same immutable id.
		current, _, rerr := ReadEnvironment(ctx, kv)
		if rerr != nil {
			return rerr
		}
		if current.ID != want {
			return fmt.Errorf("maintenance: publisher environment is already bound to %q, not %q", current.ID, want)
		}
		return nil
	}
	readback, _, err := ReadEnvironment(ctx, kv)
	if err != nil {
		return err
	}
	if readback.ID != want {
		return errors.New("maintenance: environment readback mismatch after binding")
	}
	return nil
}

// ValidateParticipantEnvironments refuses any active gateway or producer lease
// that is unbound or belongs to another environment. It deliberately checks
// every current lease, not only the operator's expected list, so a stale
// expected id cannot carry a foreign identity through the validation.
func ValidateParticipantEnvironments(ctx context.Context, gateways, producers jetstream.KeyValue, want string) error {
	expected, err := NormalizeEnvironmentID(want)
	if err != nil {
		return err
	}
	if gateways == nil || producers == nil {
		return errors.New("maintenance: participant lease buckets unavailable")
	}
	gatewayLeases, err := ActiveGatewayLeases(ctx, gateways)
	if err != nil {
		return err
	}
	for _, lease := range gatewayLeases {
		if err := RequireEnvironment("gateway "+lease.ID, lease.Environment, expected); err != nil {
			return err
		}
	}
	producerLeases, err := ActiveProducerLeases(ctx, producers)
	if err != nil {
		return err
	}
	for _, lease := range producerLeases {
		if err := RequireEnvironment("producer "+lease.ID, lease.Environment, expected); err != nil {
			return err
		}
	}
	return nil
}
