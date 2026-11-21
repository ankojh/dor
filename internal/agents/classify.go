package agents

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// ClassifyDurable is the classifier consumer name.
const ClassifyDurable = "classify"

// NewClassifier builds the category agent. It uses jev, then Claude Haiku,
// then a deterministic rule fallback that is explicitly marked degraded.
func NewClassifier(b *bus.Bus, metrics *Metrics, log *slog.Logger, deps Deps) (Agent, error) {
	c := &classifier{deps: deps, log: log}
	return NewBase(b, metrics, log, Spec{
		Name:           ClassifyDurable,
		Durable:        ClassifyDurable,
		FilterSubjects: []string{bus.SubjectArticleClean},
		MaxAckPending:  512,
		AckWait:        30 * time.Second,
		Concurrency:    4,
		Handle:         c.handle,
	})
}

type classifier struct {
	deps Deps
	log  *slog.Logger
}

func (c *classifier) handle(ctx context.Context, msg *Message, out *Emitter) error {
	var article dorv1.Article
	if err := proto.Unmarshal(msg.Data, &article); err != nil {
		return Terminal(fmt.Errorf("classify: decode article: %w", err))
	}
	if article.Id == "" {
		return Terminal(fmt.Errorf("classify: article has no id"))
	}
	result := c.classify(ctx, &article)
	data, err := proto.Marshal(result)
	if err != nil {
		return fmt.Errorf("classify: marshal: %w", err)
	}
	if err := out.Publish(ctx, bus.SubjectArticleClass, data, bus.WithMsgID("class:"+article.Id)); err != nil {
		return fmt.Errorf("classify: publish article.class: %w", err)
	}
	return nil
}

func (c *classifier) classify(ctx context.Context, article *dorv1.Article) *dorv1.ClassifiedArticle {
	lang := detectLanguage(article)
	if c.deps.Decider != nil {
		callCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		resp, err := c.deps.Decider.Decide(callCtx, decide.ClassifyRequest(decideArticle(article)))
		cancel()
		if err == nil {
			combined := decide.CombineClassify(resp, c.thresholds())
			if combined.Accepted {
				return &dorv1.ClassifiedArticle{
					Article:    article,
					Category:   categoryProto(combined.Category),
					Confidence: float32(combined.Probability),
					Accepted:   true,
					Backend:    "jev",
					Lang:       lang,
				}
			}
		} else {
			c.log.Warn("classify: jev unavailable; trying fallback", "article_id", article.Id, "err", err)
		}
	}
	if c.deps.Enricher != nil {
		if result, ok := c.fromClaude(ctx, article, lang); ok {
			return result
		}
	}
	cat, conf := ruleCategory(article)
	return &dorv1.ClassifiedArticle{
		Article:    article,
		Category:   categoryProto(cat),
		Confidence: conf,
		Accepted:   false,
		Fallback:   true,
		Degraded:   true,
		Backend:    "rule",
		Lang:       lang,
	}
}

func (c *classifier) fromClaude(ctx context.Context, article *dorv1.Article, lang string) (*dorv1.ClassifiedArticle, bool) {
	callCtx, cancel := context.WithTimeout(ctx, 1800*time.Millisecond)
	defer cancel()
	res, err := c.deps.Enricher.Complete(callCtx, llm.EnricherJob(llm.Article{
		Title:    article.GetTitle(),
		Lede:     article.GetLede(),
		Dateline: article.GetDateline(),
		Source:   article.GetSourceId(),
	}))
	if err != nil {
		c.log.Warn("classify: claude fallback failed", "article_id", article.Id, "err", err)
		return nil, false
	}
	var enrichment llm.Enrichment
	if err := res.Decode(&enrichment); err != nil {
		c.log.Warn("classify: claude output invalid", "article_id", article.Id, "err", err)
		return nil, false
	}
	cat := strings.TrimSpace(enrichment.Category)
	if !validCategory(cat) {
		return nil, false
	}
	return &dorv1.ClassifiedArticle{
		Article:    article,
		Category:   categoryProto(cat),
		Confidence: 0.8,
		Accepted:   false,
		Fallback:   true,
		Degraded:   false,
		Backend:    "claude",
		Lang:       lang,
		Entities:   dedupeStrings(enrichment.Entities),
	}, true
}

func (c *classifier) thresholds() config.Thresholds {
	t := c.deps.Thresholds
	if t.Classify.Accept == 0 && t.Plot.Primary == 0 && t.Relate.Accept == 0 {
		return decide.DefaultThresholds()
	}
	return t
}

func categoryProto(key string) dorv1.Category {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "conflict":
		return dorv1.Category_CATEGORY_CONFLICT
	case "politics":
		return dorv1.Category_CATEGORY_POLITICS
	case "economy":
		return dorv1.Category_CATEGORY_ECONOMY
	case "tech":
		return dorv1.Category_CATEGORY_TECH
	case "climate":
		return dorv1.Category_CATEGORY_CLIMATE
	case "disaster":
		return dorv1.Category_CATEGORY_DISASTER
	case "health":
		return dorv1.Category_CATEGORY_HEALTH
	case "science":
		return dorv1.Category_CATEGORY_SCIENCE
	case "sports":
		return dorv1.Category_CATEGORY_SPORTS
	case "culture":
		return dorv1.Category_CATEGORY_CULTURE
	default:
		return dorv1.Category_CATEGORY_OTHER
	}
}

func validCategory(key string) bool {
	return categoryProto(key) != dorv1.Category_CATEGORY_OTHER || strings.EqualFold(strings.TrimSpace(key), "other")
}

func ruleCategory(article *dorv1.Article) (string, float32) {
	text := strings.ToLower(article.GetTitle() + " " + article.GetLede())
	rules := []struct {
		cat   string
		words []string
	}{
		{"sports", []string{"match", "goal", "league", "tournament", "olympic", "football", "cricket", "tennis", "coach", "transfer"}},
		{"disaster", []string{"earthquake", "flood", "storm", "wildfire", "hurricane", "cyclone", "landslide", "eruption", "tsunami", "displaced"}},
		{"health", []string{"outbreak", "virus", "disease", "hospital", "vaccine", "who ", "health ministry", "infection"}},
		{"tech", []string{"software", "cyber", "hack", "ai ", "artificial intelligence", "chip", "semiconductor", "telecom", "data breach"}},
		{"economy", []string{"market", "inflation", "economy", "trade", "tariff", "stock", "bank", "oil price", "gdp", "jobs"}},
		{"conflict", []string{"war", "attack", "strike", "military", "troops", "ceasefire", "militant", "insurgent", "killed", "airstrike"}},
		{"politics", []string{"election", "president", "minister", "parliament", "government", "diplomat", "policy", "court", "lawmakers", "vote"}},
		{"climate", []string{"climate", "emissions", "warming", "environment", "renewable", "carbon", "biodiversity", "conservation"}},
		{"science", []string{"research", "space", "nasa", "study", "scientific", "discovery", "satellite", "telescope"}},
		{"culture", []string{"film", "music", "art", "festival", "religion", "celebrity", "media", "fashion", "literature"}},
	}
	for _, r := range rules {
		for _, w := range r.words {
			if strings.Contains(text, w) {
				return r.cat, 0.35
			}
		}
	}
	return "other", 0.2
}

func detectLanguage(article *dorv1.Article) string {
	if lang := strings.TrimSpace(article.GetLang()); lang != "" {
		return lang
	}
	var latin, cyrillic, arabic, han, other int
	for _, r := range article.GetTitle() + " " + article.GetLede() {
		switch {
		case r < unicode.MaxLatin1:
			latin++
		case unicode.In(r, unicode.Cyrillic):
			cyrillic++
		case unicode.In(r, unicode.Arabic):
			arabic++
		case unicode.In(r, unicode.Han):
			han++
		default:
			other++
		}
	}
	switch {
	case han > latin && han > cyrillic && han > arabic:
		return "zh"
	case cyrillic > latin && cyrillic > arabic && cyrillic > han:
		return "ru"
	case arabic > latin && arabic > cyrillic && arabic > han:
		return "ar"
	case other > latin:
		return "und"
	default:
		return "en"
	}
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[strings.ToLower(s)] {
			continue
		}
		seen[strings.ToLower(s)] = true
		out = append(out, s)
		if len(out) >= 8 {
			break
		}
	}
	return out
}
