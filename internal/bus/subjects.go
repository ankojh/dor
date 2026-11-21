// Package bus owns everything that talks to NATS: the embedded server used in
// local mode, subject names, JetStream streams, durable consumers, KV buckets
// and idempotent publishing.
package bus

import (
	"fmt"
	"hash/fnv"
)

// Subject names from the plan. Do not change them: agents, the gateway and the
// eval tooling all bind to these strings.
const (
	SubjectArticleRaw         = "article.raw"
	SubjectArticleRawPriority = "article.raw.priority"
	SubjectArticleClean       = "article.clean"
	SubjectArticlePlace       = "article.place"
	SubjectArticleClass       = "article.class"
	SubjectArticleVec         = "article.vec"
	SubjectLinkChanged        = "link.changed"
	SubjectArticlePlaced      = "article.placed"
	SubjectMatchAlert         = "match.alert"
	SubjectDeltaOut           = "delta.out"

	// SubjectStoryChangedPrefix is followed by the shard number.
	SubjectStoryChangedPrefix = "story.changed."
	// SubjectMatchFilterPrefix is followed by the client id.
	SubjectMatchFilterPrefix = "match.filter."

	// Dev-only sample subjects for the echo agent.
	SubjectEchoIn  = "echo.in"
	SubjectEchoOut = "echo.out"

	// SubjectDLQPrefix is followed by the agent name.
	SubjectDLQPrefix = "dlq."
)

// StoryShardCount is the number of story.changed shards.
const StoryShardCount = 16

// StoryShard returns the shard for a story id: FNV-1a 32-bit mod 16.
func StoryShard(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % StoryShardCount)
}

// StoryChangedSubject returns the ordered subject for a story id.
func StoryChangedSubject(id string) string {
	return fmt.Sprintf("%s%d", SubjectStoryChangedPrefix, StoryShard(id))
}

// StoryChangedSubjectForShard returns story.changed.<shard>.
func StoryChangedSubjectForShard(shard int) string {
	return fmt.Sprintf("%s%d", SubjectStoryChangedPrefix, shard)
}

// MatchFilterSubject returns match.filter.<client>.
func MatchFilterSubject(client string) string {
	return SubjectMatchFilterPrefix + client
}

// DLQSubject returns dlq.<agent>.
func DLQSubject(agent string) string {
	return SubjectDLQPrefix + agent
}

// IsPipelineSubject reports whether subject belongs to one of the v1 streams.
func IsPipelineSubject(subject string) bool {
	return StreamForSubject(subject) != ""
}
