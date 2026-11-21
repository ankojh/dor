// Package matcher evaluates prompt filters and alert prompts. Prompt vectors
// and article vectors are cached; jev is only called after a cheap embedding
// or lexical prefilter, so a provider outage degrades to approximate scores
// instead of blocking the wire. The gateway never calls a provider.
package matcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/alert"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/embed"
	"github.com/ankojh/dor/internal/live"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// Decider is the jev surface used here.
type Decider interface {
	Decide(ctx context.Context, req decide.Request) (decide.Response, error)
}

// Service owns the shared prompt evaluation cache.
type Service struct {
	bus        *bus.Bus
	log        *slog.Logger
	decider    Decider
	embedder   *embed.Client
	alerts     *alert.Store
	thresholds config.Thresholds
	now        func() time.Time

	kvOnce sync.Once
	kv     jetstream.KeyValue
	kvErr  error

	mu              sync.Mutex
	promptVecs      map[string][]float32
	articleVec      map[string]articleVector
	prompts         []string
	clients         map[string][]string
	alertsList      []alert.Alert
	last            time.Time
	lastBackfill    time.Time
	backfillSig     string
	backfilling     bool
	backfillPending bool
	backfillCancel  context.CancelFunc
	clientVersion   uint64
	embedRetryAfter time.Time
	backfillResult  WindowResult
}

type articleVector struct {
	vec []float32
	at  time.Time
}

type promptsRecord struct {
	ClientID  string   `json:"client_id"`
	Prompts   []string `json:"prompts"`
	UpdatedAt int64    `json:"updated_at"`
}

// BacktestStory is one story in an alert backtest result.
type BacktestStory struct {
	StoryID     string  `json:"story_id"`
	Title       string  `json:"title"`
	SourceCount int     `json:"source_count"`
	Score       float64 `json:"score"`
	Approx      bool    `json:"approx"`
}

// BacktestResult is the alert preview.
type BacktestResult struct {
	WouldFire int             `json:"would_fire"`
	Stories   []BacktestStory `json:"stories"`
	Approx    bool            `json:"approx"`
	TookMS    int64           `json:"took_ms"`
	WindowResult
}

// WindowResult describes coverage independently of the preview's display limit.
type WindowResult struct {
	Partial   bool  `json:"partial"`
	Scanned   int   `json:"scanned"`
	Eligible  int   `json:"eligible"`
	Evaluated int   `json:"evaluated"`
	From      int64 `json:"from"`
	To        int64 `json:"to"`
}

// New builds the matcher service.
func New(b *bus.Bus, log *slog.Logger, decider Decider, embedder *embed.Client, alerts *alert.Store, thresholds config.Thresholds) *Service {
	if log == nil {
		log = slog.Default()
	}
	if thresholds.Match.Strictness == 0 {
		thresholds = config.DefaultThresholds()
	}
	return &Service{
		bus:        b,
		log:        log,
		decider:    decider,
		embedder:   embedder,
		alerts:     alerts,
		thresholds: thresholds,
		now:        time.Now,
		promptVecs: make(map[string][]float32),
		articleVec: make(map[string]articleVector),
		clients:    make(map[string][]string),
	}
}

func (s *Service) keyValue(ctx context.Context) jetstream.KeyValue {
	s.kvOnce.Do(func() {
		s.kv, s.kvErr = s.bus.KV(ctx, bus.KVBucketLiveState)
		if s.kvErr != nil {
			s.log.Warn("matcher: live_state unavailable", "err", s.kvErr)
		}
	})
	return s.kv
}

// SetClientPrompts stores one client's active prompts for the matcher and
// makes them live without restarting any consumer.
func (s *Service) SetClientPrompts(ctx context.Context, clientID string, prompts []string) {
	clean := cleanPrompts(prompts)
	rec := promptsRecord{ClientID: clientID, Prompts: clean, UpdatedAt: s.now().Unix()}
	kv := s.keyValue(ctx)
	if kv != nil {
		key := "prompts." + sanitize(clientID)
		if data, err := json.Marshal(rec); err == nil {
			opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
			_, _ = kv.Put(opCtx, key, data)
			cancel()
		}
	}
	s.mu.Lock()
	s.clients[clientID] = clean
	s.clientVersion++
	s.rebuildPromptsLocked()
	s.mu.Unlock()
	s.maybeBackfill(context.WithoutCancel(ctx))
}

// ClearClientPrompts removes a disconnected client's prompt set so the
// matcher stops evaluating prompts no client is watching.
func (s *Service) ClearClientPrompts(ctx context.Context, clientID string) {
	kv := s.keyValue(ctx)
	if kv != nil {
		opCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_ = kv.Delete(opCtx, "prompts."+sanitize(clientID))
		cancel()
	}
	s.mu.Lock()
	delete(s.clients, clientID)
	s.clientVersion++
	s.rebuildPromptsLocked()
	s.mu.Unlock()
}

// Refresh loads all clients' prompts and the saved alerts. It is cheap enough
// to call every few seconds.
func (s *Service) Refresh(ctx context.Context) {
	s.mu.Lock()
	version := s.clientVersion
	s.mu.Unlock()
	kv := s.keyValue(ctx)
	clients := make(map[string][]string)
	loaded := false
	if kv != nil {
		scanCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := s.window(scanCtx, "prompts.", func(e jetstream.KeyValueEntry) {
			var rec promptsRecord
			if json.Unmarshal(e.Value(), &rec) == nil && rec.ClientID != "" {
				clients[rec.ClientID] = cleanPrompts(rec.Prompts)
			}
		})
		cancel()
		loaded = err == nil
	}
	var alerts []alert.Alert
	if s.alerts != nil {
		alerts = s.alerts.List(ctx)
	}
	s.mu.Lock()
	if version == s.clientVersion && loaded {
		s.clients = clients
	}
	s.alertsList = alerts
	s.rebuildPromptsLocked()
	prompts := append([]string(nil), s.prompts...)
	s.last = s.now()
	s.mu.Unlock()
	s.maybeBackfill(ctx)
	s.ensurePromptVectors(ctx, prompts)
}

func (s *Service) rebuildPromptsLocked() {
	seen := make(map[string]bool)
	var prompts []string
	for _, list := range s.clients {
		for _, p := range list {
			if !seen[p] {
				seen[p] = true
				prompts = append(prompts, p)
			}
		}
	}
	sort.Strings(prompts)
	s.prompts = prompts
}

// ensurePromptVectors embeds prompts missing a vector. It runs outside the
// service lock so a slow TEI call cannot stall article scoring. A failure
// leaves the prompts on the lexical path.
func (s *Service) ensurePromptVectors(ctx context.Context, prompts []string) {
	s.mu.Lock()
	retry := s.embedRetryAfter
	s.mu.Unlock()
	if !s.embedder.Enabled() || s.now().Before(retry) {
		return
	}
	s.mu.Lock()
	var missing []string
	for _, p := range prompts {
		if _, ok := s.promptVecs[p]; !ok {
			missing = append(missing, p)
		}
	}
	s.mu.Unlock()
	for start := 0; start < len(missing); start += 16 {
		end := start + 16
		if end > len(missing) {
			end = len(missing)
		}
		embedCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		vecs, err := s.embedder.Embed(embedCtx, missing[start:end])
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.mu.Lock()
				s.embedRetryAfter = s.now().Add(10 * time.Second)
				s.mu.Unlock()
			}
			s.log.Debug("matcher: prompt embedding unavailable; lexical prefilter", "err", err)
			return
		}
		s.mu.Lock()
		for i, v := range vecs {
			if i < end-start {
				s.promptVecs[missing[start+i]] = v
			}
		}
		s.mu.Unlock()
	}
}

// maybeBackfill only runs when the prompt set changed or five minutes passed,
// so the five-second refresh does not re-score the last hour constantly.
func (s *Service) maybeBackfill(ctx context.Context) {
	s.mu.Lock()
	if len(s.prompts) == 0 {
		s.mu.Unlock()
		return
	}
	sig := promptSignature(s.prompts)
	if sig == s.backfillSig && s.now().Sub(s.lastBackfill) < 5*time.Minute {
		s.mu.Unlock()
		return
	}
	s.backfillSig = sig
	s.lastBackfill = s.now()
	s.mu.Unlock()
	s.backfill(ctx)
}

// ArticleScores evaluates every active prompt against one article and returns
// the scores and the prompts scored without jev.
func (s *Service) ArticleScores(ctx context.Context, a *dorv1.Article) (map[string]float32, []string) {
	if a == nil || a.Id == "" {
		return nil, nil
	}
	s.mu.Lock()
	prompts := append([]string(nil), s.prompts...)
	s.mu.Unlock()
	if len(prompts) == 0 {
		return nil, nil
	}
	text := strings.TrimSpace(a.Title + ". " + a.Lede)
	vec := s.articleVector(ctx, a.Id, text)
	scores := make(map[string]float32)
	var approx []string
	for _, prompt := range prompts {
		s.mu.Lock()
		pv := s.promptVecs[prompt]
		s.mu.Unlock()
		pre := 0.0
		if len(pv) > 0 && len(vec) > 0 {
			pre = cosine(pv, vec)
			if pre < 0.20 {
				continue
			}
		} else {
			pre = lexicalCoverage(prompt, text)
			if pre < 0.12 {
				continue
			}
		}
		score, answered := s.jevMatch(ctx, decide.Story{
			ID:    a.Id,
			Title: a.Title,
			Brief: a.Lede,
			Place: hintPlace(a),
		}, prompt, s.thresholds.Match.Strictness)
		if answered {
			// Scores below the prompt threshold are noise on the wire; the
			// client dims rows above zero, so do not send every near miss.
			if score >= 0.55 {
				scores[prompt] = float32(score)
			}
			continue
		}
		approx = append(approx, prompt)
		scores[prompt] = float32(pre)
	}
	if len(scores) == 0 {
		return nil, nil
	}
	return scores, approx
}

// StoryAlerts re-checks every enabled alert when a story changes.
func (s *Service) StoryAlerts(ctx context.Context, story *dorv1.Story) ([]alert.Hit, error) {
	if story == nil || story.Id == "" || story.SourceCount < 1 {
		return nil, nil
	}
	s.mu.Lock()
	alerts := append([]alert.Alert(nil), s.alertsList...)
	s.mu.Unlock()
	var hits []alert.Hit
	for _, a := range alerts {
		if !a.Enabled || int(story.SourceCount) < a.MinSources || (a.Region != "" && !storyRegion(story, a.Region)) {
			continue
		}
		if s.alerts != nil && s.alerts.IsMuted(ctx, a.ID, story.Id) {
			continue
		}
		generation := uint64(0)
		base := int(story.SourceCount)
		previous := false
		if s.alerts != nil {
			prev, ok, err := s.alerts.HitState(ctx, a.ID, story.Id)
			if err != nil {
				return nil, err
			}
			if ok {
				previous = true
				generation = prev.Generation + 1
				base = prev.BaseSources
				if base <= 0 {
					base = max(1, prev.SourceCount>>min(prev.Generation, 32))
				}
				if generation > 32 || uint64(base)<<generation > uint64(story.SourceCount) {
					continue
				}
			}
		}
		input := s.storyInput(ctx, story)
		score, answered := s.jevMatch(ctx, input, a.Prompt, a.Strictness)
		if !answered {
			score = s.approxMatch(ctx, input, a.Prompt, a.Strictness)
		}
		if score < a.Strictness {
			continue
		}
		for {
			level := "new"
			if previous {
				level = "escalated"
			}
			hits = append(hits, alert.Hit{AlertID: a.ID, StoryID: story.Id, Level: level, Generation: generation, BaseSources: base,
				FiredAt: s.now().UTC(), Title: story.Title, Prompt: a.Prompt, SourceCount: int(story.SourceCount), Approx: !answered})
			if !previous || generation >= 32 || uint64(base)<<(generation+1) > uint64(story.SourceCount) {
				break
			}
			generation++
		}
	}
	return hits, nil
}

// RecordHit persists a hit only after the alert delta has been published. If
// the publish fails, the ordered story consumer retries and can re-evaluate;
// recording first would make the retry look like a duplicate and lose the
// notification.
func (s *Service) RecordHit(ctx context.Context, hit alert.Hit) error {
	if s.alerts != nil {
		return s.alerts.RecordHit(ctx, hit)
	}
	return nil
}

// Backtest evaluates a draft alert against the last 24 h of stories.
func (s *Service) Backtest(ctx context.Context, prompt string, minSources int, region string, strictness float64) BacktestResult {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if strictness <= 0 || strictness > 1 {
		strictness = .80
	}
	if minSources <= 0 {
		minSources = 1
	}
	end := s.now().Unix()
	cutoff := s.now().Add(-24 * time.Hour).Unix()
	res := BacktestResult{Stories: []BacktestStory{}, Approx: s.decider == nil, WindowResult: WindowResult{From: cutoff, To: end}}
	var stories []*dorv1.Story
	err := s.window(ctx, live.StoryPrefix, func(e jetstream.KeyValueEntry) {
		res.Scanned++
		var st dorv1.Story
		if proto.Unmarshal(e.Value(), &st) != nil {
			res.Partial = true
			return
		}
		at := max(st.FirstSeen, st.UpdatedAt)
		if at == 0 {
			at = e.Created().Unix()
		}
		if at < cutoff || at > end || int(st.SourceCount) < minSources || (region != "" && !storyRegion(&st, region)) {
			return
		}
		res.Eligible++
		stories = append(stories, &st)
	})
	if err != nil {
		res.Partial = true
	}
	sort.Slice(stories, func(i, j int) bool {
		return max(stories[i].FirstSeen, stories[i].UpdatedAt) > max(stories[j].FirstSeen, stories[j].UpdatedAt)
	})
	s.ensurePromptVectors(ctx, []string{prompt})
	for _, st := range stories {
		if ctx.Err() != nil {
			res.Partial = true
			break
		}
		input := s.storyInput(ctx, st)
		score, answered := s.jevMatch(ctx, input, prompt, strictness)
		if !answered {
			res.Approx = true
			score = s.approxMatch(ctx, input, prompt, strictness)
		}
		if ctx.Err() != nil {
			res.Partial = true
			break
		}
		res.Evaluated++
		if score < strictness {
			continue
		}
		res.WouldFire++
		if len(res.Stories) < 20 {
			res.Stories = append(res.Stories, BacktestStory{StoryID: st.Id, Title: st.Title, SourceCount: int(st.SourceCount), Score: score, Approx: !answered})
		}
	}
	res.TookMS = time.Since(start).Milliseconds()
	return res
}

// window streams a prefix-filtered snapshot, including every page, rather
// than truncating a bucket-wide key list. The deadline bounds work explicitly.
func (s *Service) window(ctx context.Context, prefix string, visit func(jetstream.KeyValueEntry)) error {
	kv := s.keyValue(ctx)
	if kv == nil {
		return fmt.Errorf("matcher: live state unavailable")
	}
	w, err := kv.Watch(ctx, prefix+">", jetstream.IgnoreDeletes())
	if err != nil {
		return err
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-w.Updates():
			if !ok {
				return fmt.Errorf("matcher: window scan interrupted")
			}
			if e == nil {
				return nil
			}
			visit(e)
		}
	}
}

// approxMatch requires strong prompt coverage or high semantic similarity.
// Lexical evidence lowers the semantic cutoff; these scores are not probabilities.
func (s *Service) approxMatch(ctx context.Context, in decide.Story, prompt string, strictness float64) float64 {
	text := strings.TrimSpace(in.Title + " " + in.Brief + " " + in.Place + " " + in.Category)
	coverage := lexicalCoverage(prompt, text)
	s.ensurePromptVectors(ctx, []string{prompt})
	s.mu.Lock()
	pv := s.promptVecs[prompt]
	s.mu.Unlock()
	score := coverage
	if len(pv) > 0 {
		vec := s.articleVector(ctx, "story:"+in.ID+":"+promptSignature([]string{text}), text)
		semantic := cosine(pv, vec)
		if semantic >= max(.88, strictness) || (coverage >= .25 && semantic >= max(.82, strictness)) {
			score = max(score, semantic)
		}
	}
	if score < max(.80, strictness) {
		return 0
	}
	return score
}

// PublishScores sends one MatchScore for the publisher to fold into a wire
// row. A repeated evaluation with identical scores dedupes in JetStream.
func (s *Service) PublishScores(ctx context.Context, articleID string, scores map[string]float32, approx []string) error {
	if articleID == "" || len(scores) == 0 {
		return nil
	}
	msg := &dorv1.MatchScore{ArticleId: articleID, Scores: scores, Approx: approx, UpdatedAt: s.now().Unix()}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return err
	}
	id := scoreID(articleID, scores, approx)
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = s.bus.Publish(pubCtx, bus.SubjectMatchScore, data, bus.WithMsgID(id))
	if err != nil {
		s.log.Debug("matcher: publish score failed", "article", articleID, "err", err)
	}
	return err
}

// BackfillNew evaluates recent articles for prompts that just arrived. It is
// bounded and runs in the background; the publisher only re-emits wire rows it
// still remembers.
func (s *Service) backfill(ctx context.Context) {
	s.mu.Lock()
	if len(s.prompts) == 0 {
		s.mu.Unlock()
		return
	}
	if s.backfilling {
		s.backfillPending = true
		cancel := s.backfillCancel
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}
	s.backfilling = true
	s.mu.Unlock()
	go func() {
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		for {
			passCtx, passCancel := context.WithCancel(runCtx)
			s.mu.Lock()
			prompts := append([]string(nil), s.prompts...)
			s.backfillCancel = passCancel
			s.backfillPending = false
			s.mu.Unlock()
			s.ensurePromptVectors(passCtx, prompts)
			s.backfillOnce(passCtx)
			passCancel()
			s.mu.Lock()
			s.backfillCancel = nil
			if !s.backfillPending || runCtx.Err() != nil {
				s.backfilling = false
				s.backfillPending = false
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		}
	}()
}

func (s *Service) backfillOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	result := WindowResult{From: s.now().Add(-time.Hour).Unix(), To: s.now().Unix()}
	var articles []*dorv1.Article
	err := s.window(ctx, live.ArticlePrefix, func(e jetstream.KeyValueEntry) {
		result.Scanned++
		var a dorv1.Article
		if proto.Unmarshal(e.Value(), &a) != nil {
			result.Partial = true
			return
		}
		at := a.PublishedAt
		if at == 0 {
			at = e.Created().Unix()
		}
		if at < result.From || at > result.To {
			return
		}
		result.Eligible++
		articles = append(articles, &a)
	})
	if err != nil {
		result.Partial = true
	}
	sort.Slice(articles, func(i, j int) bool { return articles[i].PublishedAt > articles[j].PublishedAt })
	for _, a := range articles {
		if ctx.Err() != nil {
			result.Partial = true
			break
		}
		scores, approx := s.ArticleScores(ctx, a)
		if len(scores) > 0 {
			if err := s.PublishScores(ctx, a.Id, scores, approx); err != nil {
				result.Partial = true
			}
		}
		if ctx.Err() != nil {
			result.Partial = true
			break
		}
		result.Evaluated++
	}
	s.mu.Lock()
	s.backfillResult = result
	if result.Partial {
		s.lastBackfill = time.Time{}
	} else {
		s.lastBackfill = s.now()
	}
	s.mu.Unlock()
	// Shared coverage metadata is available even when this matcher runs outside the gateway process.
	if kv := s.keyValue(ctx); kv != nil {
		data, _ := json.Marshal(result)
		reportCtx, reportCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		_, _ = kv.Put(reportCtx, "matcher.backfill", data)
		reportCancel()
	}
	s.log.Info("matcher: backfill completed", "partial", result.Partial, "scanned", result.Scanned, "eligible", result.Eligible, "evaluated", result.Evaluated)
}

// BackfillStatus reports coverage of the last completed one-hour backfill.
func (s *Service) BackfillStatus() WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backfillResult
}

// WatchPrompts makes registrations from other gateway processes live without
// waiting for the periodic alert refresh. Reconnecting replays current keys.
func (s *Service) WatchPrompts(ctx context.Context) error {
	kv := s.keyValue(ctx)
	if kv == nil {
		return fmt.Errorf("matcher: live state unavailable")
	}
	w, err := kv.Watch(ctx, "prompts.>")
	if err != nil {
		return err
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-w.Updates():
			if !ok {
				return fmt.Errorf("matcher: prompt watcher interrupted")
			}
			if e == nil {
				continue
			}
			var rec promptsRecord
			s.mu.Lock()
			if e.Operation() == jetstream.KeyValuePut && json.Unmarshal(e.Value(), &rec) == nil && rec.ClientID != "" {
				s.clients[rec.ClientID] = cleanPrompts(rec.Prompts)
			} else if e.Operation() != jetstream.KeyValuePut {
				for id := range s.clients {
					if "prompts."+sanitize(id) == e.Key() {
						delete(s.clients, id)
					}
				}
			}
			s.clientVersion++
			s.rebuildPromptsLocked()
			s.mu.Unlock()
			s.maybeBackfill(ctx)
		}
	}
}

func (s *Service) articleVector(ctx context.Context, id, text string) []float32 {
	s.mu.Lock()
	if v, ok := s.articleVec[id]; ok && s.now().Sub(v.at) < 30*time.Minute {
		vec := v.vec
		s.mu.Unlock()
		return vec
	}
	s.mu.Unlock()
	if !s.embedder.Enabled() {
		return nil
	}
	s.mu.Lock()
	retry := s.embedRetryAfter
	s.mu.Unlock()
	if s.now().Before(retry) {
		return nil
	}
	embedCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	vecs, err := s.embedder.Embed(embedCtx, []string{text})
	cancel()
	if err != nil || len(vecs) == 0 {
		if ctx.Err() == nil {
			s.mu.Lock()
			s.embedRetryAfter = s.now().Add(10 * time.Second)
			s.mu.Unlock()
		}
		return nil
	}
	s.mu.Lock()
	if len(s.articleVec) > 2000 {
		s.articleVec = make(map[string]articleVector)
	}
	s.articleVec[id] = articleVector{vec: vecs[0], at: s.now()}
	s.mu.Unlock()
	return vecs[0]
}

// jevMatch returns the match probability and whether jev answered. A missing
// decider is answered=false, which takes the approximate path.
func (s *Service) jevMatch(ctx context.Context, story decide.Story, prompt string, strictness float64) (float64, bool) {
	if s.decider == nil {
		return 0, false
	}
	providerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	resp, err := s.decider.Decide(providerCtx, decide.MatchRequest(story, prompt))
	if err != nil {
		return 0, false
	}
	res := decide.CombineMatch(resp, s.thresholds)
	if !res.Answered {
		return 0, false
	}
	if res.MatchP < strictness {
		return res.MatchP, true
	}
	return res.MatchP, true
}

// storyInput includes the cached brief when it exists; the brief materially
// improves jev matching over a bare title.
func (s *Service) storyInput(ctx context.Context, story *dorv1.Story) decide.Story {
	in := storyInput(story)
	if kv := s.keyValue(ctx); kv != nil {
		if text, ok := live.LoadStoryText(ctx, kv, story.Id); ok && strings.TrimSpace(text.Brief) != "" {
			in.Brief = text.Brief
		}
	}
	return in
}

func storyInput(s *dorv1.Story) decide.Story {
	return decide.Story{
		ID:       s.Id,
		Title:    s.Title,
		Brief:    s.Title,
		Place:    storyPlace(s),
		Category: strings.ToLower(strings.TrimPrefix(s.Category.String(), "CATEGORY_")),
	}
}

func storyPlace(s *dorv1.Story) string {
	if s == nil || len(s.Locations) == 0 {
		return ""
	}
	idx := int(s.Primary)
	if idx < 0 || idx >= len(s.Locations) {
		idx = 0
	}
	return s.Locations[idx].Name
}

func storyRegion(s *dorv1.Story, region string) bool {
	region = strings.ToLower(strings.TrimSpace(region))
	if region == "" {
		return true
	}
	for _, loc := range s.Locations {
		if strings.Contains(strings.ToLower(loc.Name), region) {
			return true
		}
	}
	return false
}

func hintPlace(a *dorv1.Article) string {
	if a.Hints != nil && a.Hints.Place != "" {
		return a.Hints.Place
	}
	return a.Dateline
}

func cleanPrompts(in []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(in))
	for _, p := range in {
		p = strings.TrimSpace(p)
		if p == "" || len(p) > 1000 || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func sanitize(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '=':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "client"
	}
	return b.String()
}

func promptSignature(prompts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(prompts, "\x00")))
	return hex.EncodeToString(sum[:12])
}

func scoreID(articleID string, scores map[string]float32, approx []string) string {
	keys := make([]string, 0, len(scores))
	for k := range scores {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(articleID)
	for _, k := range keys {
		fmt.Fprintf(&b, "|%s=%.4f", k, scores[k])
	}
	for _, a := range approx {
		b.WriteString("|~" + a)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "score:" + hex.EncodeToString(sum[:12])
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func lexical(a, b string) float64 {
	as := tokens(a)
	bs := tokens(b)
	if len(as) == 0 || len(bs) == 0 {
		return 0
	}
	shared := 0
	for t := range as {
		if bs[t] {
			shared++
		}
	}
	return float64(shared) / math.Sqrt(float64(len(as)*len(bs)))
}

func lexicalCoverage(prompt, text string) float64 {
	query, words := tokens(prompt), tokens(text)
	if len(query) == 0 {
		return 0
	}
	shared := 0
	for t := range query {
		if words[t] {
			shared++
		}
	}
	return float64(shared) / float64(len(query))
}

func tokens(s string) map[string]bool {
	out := make(map[string]bool)
	stop := map[string]bool{"the": true, "and": true, "for": true, "with": true, "about": true, "from": true, "that": true, "this": true, "news": true, "stories": true, "are": true, "any": true, "please": true, "show": true, "new": true, "all": true, "where": true, "when": true}
	for _, w := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		if len(w) <= 2 || stop[w] {
			continue
		}
		if len(w) > 4 && strings.HasSuffix(w, "s") {
			w = strings.TrimSuffix(w, "s")
		}
		out[w] = true
	}
	return out
}
