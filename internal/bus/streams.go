package bus

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Stream names. One stream per delivery domain: retention, capacity, dedupe
// window and message-id scope belong to the domain, not to the whole pipeline.
const (
	StreamIngest = "INGEST"
	StreamEnrich = "ENRICH"
	StreamStory  = "STORY"
	StreamAlert  = "ALERT"
	StreamDelta  = "DELTA"
	StreamMatch  = "MATCH"
	StreamDLQ    = "DLQ"
	StreamEcho   = "ECHO"
)

const (
	MiB int64 = 1 << 20
	GiB int64 = 1 << 30
)

// Retention and size defaults. They are sized for local development; MaxBytes
// is the binding limit before MaxAge under the 50 articles/s design load. See
// docs/decisions.md for the storage budget.
const (
	DefaultIngestMaxAge   = 6 * time.Hour
	DefaultEnrichMaxAge   = 6 * time.Hour
	DefaultStoryMaxAge    = 48 * time.Hour
	DefaultAlertMaxAge    = 7 * 24 * time.Hour
	DefaultDeltaMaxAge    = 10 * time.Minute
	DefaultMatchMaxAge    = 2 * time.Minute
	DefaultDLQMaxAge      = 7 * 24 * time.Hour
	DefaultEchoMaxAge     = time.Hour
	DefaultProcessingTTL  = 20 * time.Minute
	DefaultDeltaDedupe    = 10 * time.Minute
	DefaultMatchDedupe    = 2 * time.Minute
	DefaultIngestMaxBytes = 2 * GiB
	DefaultEnrichMaxBytes = 2 * GiB
	DefaultStoryMaxBytes  = 1 * GiB
	DefaultAlertMaxBytes  = 1 * GiB
	DefaultDeltaMaxBytes  = 256 * MiB
	DefaultMatchMaxBytes  = 128 * MiB
	DefaultDLQMaxBytes    = 1 * GiB
	DefaultEchoMaxBytes   = 128 * MiB
)

// StreamPolicy is the provisioned policy of one stream. A zero field means
// "use the default" when supplied as an override.
type StreamPolicy struct {
	Subjects   []string
	Storage    jetstream.StorageType
	MaxAge     time.Duration
	MaxBytes   int64
	Duplicates time.Duration
}

// DefaultStreamPolicies returns a fresh map of the v1 stream policies.
func DefaultStreamPolicies() map[string]StreamPolicy {
	return map[string]StreamPolicy{
		StreamIngest: {
			Subjects:   []string{SubjectArticleRaw, SubjectArticleRawPriority},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultIngestMaxAge,
			MaxBytes:   DefaultIngestMaxBytes,
			Duplicates: DefaultProcessingTTL,
		},
		StreamEnrich: {
			Subjects: []string{
				SubjectArticleClean,
				SubjectArticlePlace,
				SubjectArticleClass,
				SubjectArticleVec,
				SubjectArticlePlaced,
			},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultEnrichMaxAge,
			MaxBytes:   DefaultEnrichMaxBytes,
			Duplicates: DefaultProcessingTTL,
		},
		StreamStory: {
			Subjects:   []string{"story.changed.*", SubjectLinkChanged},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultStoryMaxAge,
			MaxBytes:   DefaultStoryMaxBytes,
			Duplicates: DefaultProcessingTTL,
		},
		StreamAlert: {
			Subjects:   []string{SubjectMatchAlert},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultAlertMaxAge,
			MaxBytes:   DefaultAlertMaxBytes,
			Duplicates: DefaultProcessingTTL,
		},
		StreamDelta: {
			Subjects:   []string{SubjectDeltaOut},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultDeltaMaxAge,
			MaxBytes:   DefaultDeltaMaxBytes,
			Duplicates: DefaultDeltaDedupe,
		},
		StreamMatch: {
			Subjects:   []string{"match.filter.*"},
			Storage:    jetstream.MemoryStorage,
			MaxAge:     DefaultMatchMaxAge,
			MaxBytes:   DefaultMatchMaxBytes,
			Duplicates: DefaultMatchDedupe,
		},
		StreamDLQ: {
			Subjects:   []string{"dlq.>"},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultDLQMaxAge,
			MaxBytes:   DefaultDLQMaxBytes,
			Duplicates: DefaultProcessingTTL,
		},
		StreamEcho: {
			Subjects:   []string{"echo.>"},
			Storage:    jetstream.FileStorage,
			MaxAge:     DefaultEchoMaxAge,
			MaxBytes:   DefaultEchoMaxBytes,
			Duplicates: DefaultProcessingTTL,
		},
	}
}

// StreamOrder is the deterministic provisioning order.
func StreamOrder() []string {
	return []string{
		StreamIngest,
		StreamEnrich,
		StreamStory,
		StreamAlert,
		StreamDelta,
		StreamMatch,
		StreamDLQ,
		StreamEcho,
	}
}

// StreamForSubject returns the stream that carries subject or filter.
func StreamForSubject(subject string) string {
	switch {
	case subject == SubjectArticleRaw, subject == SubjectArticleRawPriority:
		return StreamIngest
	case subject == SubjectArticleClean,
		subject == SubjectArticlePlace,
		subject == SubjectArticleClass,
		subject == SubjectArticleVec,
		subject == SubjectArticlePlaced:
		return StreamEnrich
	case strings.HasPrefix(subject, SubjectStoryChangedPrefix),
		subject == "story.changed.*",
		subject == "story.changed.>",
		subject == SubjectLinkChanged:
		return StreamStory
	case subject == SubjectMatchAlert:
		return StreamAlert
	case subject == SubjectDeltaOut:
		return StreamDelta
	case strings.HasPrefix(subject, SubjectMatchFilterPrefix), subject == "match.filter.*":
		return StreamMatch
	case strings.HasPrefix(subject, "dlq."):
		return StreamDLQ
	case strings.HasPrefix(subject, "echo."):
		return StreamEcho
	default:
		return ""
	}
}

// StreamForFilters returns the single stream that carries every filter. It is
// an error for an agent to span streams in one consumer.
func StreamForFilters(filters []string) (string, error) {
	var stream string
	for _, filter := range filters {
		s := StreamForSubject(filter)
		if s == "" {
			return "", fmt.Errorf("bus: subject %q belongs to no stream", filter)
		}
		if stream == "" {
			stream = s
			continue
		}
		if stream != s {
			return "", fmt.Errorf("bus: filters span streams %s and %s (%q)", stream, s, filter)
		}
	}
	if stream == "" {
		return "", fmt.Errorf("bus: no filter subjects")
	}
	return stream, nil
}

// SubjectsForStream returns the configured subjects for a stream.
func SubjectsForStream(policy StreamPolicy) []string {
	return append([]string(nil), policy.Subjects...)
}

// StreamSubjects returns every subject bound by any stream. Kept for callers
// that need the full pipeline surface; stream-specific code should use
// StreamForSubject.
func StreamSubjects() []string {
	policies := DefaultStreamPolicies()
	var out []string
	for _, name := range StreamOrder() {
		out = append(out, policies[name].Subjects...)
	}
	sort.Strings(out)
	return out
}

// StreamMatches reports whether subject is captured by a policy subject
// pattern. It supports the '*' and '>' wildcards used by the stream config.
func StreamMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	if strings.HasSuffix(pattern, ".>") {
		return strings.HasPrefix(subject, strings.TrimSuffix(pattern, ">"))
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(subject, prefix) && !strings.Contains(strings.TrimPrefix(subject, prefix), ".")
	}
	return false
}

// policyConfig turns a policy into a JetStream stream config.
func policyConfig(name string, policy StreamPolicy) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        name,
		Subjects:    policy.Subjects,
		Retention:   jetstream.LimitsPolicy,
		Storage:     policy.Storage,
		MaxAge:      policy.MaxAge,
		MaxBytes:    policy.MaxBytes,
		Duplicates:  policy.Duplicates,
		Discard:     jetstream.DiscardOld,
		AllowDirect: true,
	}
}
