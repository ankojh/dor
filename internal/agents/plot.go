package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"google.golang.org/protobuf/proto"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/bus"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/geo"
	"github.com/ankojh/dor/internal/llm"
	"github.com/ankojh/dor/internal/pb/dorv1"
)

// PlotDurable is the plotter consumer name.
const PlotDurable = "plot"

// NewPlotter builds the place agent. It always starts: without jev, Claude,
// Photon or a loaded gazetteer it emits an explicitly degraded deterministic
// result (or no dot for global).
func NewPlotter(b *bus.Bus, metrics *Metrics, log *slog.Logger, deps Deps) (Agent, error) {
	p := &plotter{deps: deps, log: log}
	return NewBase(b, metrics, log, Spec{
		Name:           PlotDurable,
		Durable:        PlotDurable,
		FilterSubjects: []string{bus.SubjectArticleClean},
		MaxAckPending:  512,
		AckWait:        30 * time.Second,
		Concurrency:    4,
		Handle:         p.handle,
	})
}

type plotter struct {
	deps Deps
	log  *slog.Logger
}

func (p *plotter) handle(ctx context.Context, msg *Message, out *Emitter) error {
	var article dorv1.Article
	if err := proto.Unmarshal(msg.Data, &article); err != nil {
		return Terminal(fmt.Errorf("plot: decode article: %w", err))
	}
	if article.Id == "" {
		return Terminal(fmt.Errorf("plot: article has no id"))
	}
	placed := p.place(ctx, &article)
	data, err := proto.Marshal(placed)
	if err != nil {
		return fmt.Errorf("plot: marshal: %w", err)
	}
	if err := out.Publish(ctx, bus.SubjectArticlePlace, data, bus.WithMsgID("place:"+article.Id)); err != nil {
		return fmt.Errorf("plot: publish article.place: %w", err)
	}
	return nil
}

func (p *plotter) place(ctx context.Context, article *dorv1.Article) *dorv1.PlacedArticle {
	res := &dorv1.PlacedArticle{Article: article, Primary: 0}
	hints := article.GetHints()
	countryHint := ""
	if hints != nil {
		countryHint = strings.TrimSpace(hints.GetCountry())
		if countryHint == "" {
			countryHint = strings.TrimSpace(hints.GetPlace())
		}
	}
	// A hazard or GDELT item with coordinates is already the event location;
	// use it first and never spend a model call just to move the dot.
	if hints != nil && hints.GetLocated() && hints.GetHazard() != "" {
		res.Locations = []*dorv1.Location{{
			PlaceId:   "hint:" + firstNonEmpty(hints.GetEventId(), article.GetId()),
			Name:      firstNonEmpty(hints.GetPlace(), article.GetDateline(), countryHint, "Hazard location"),
			Lat:       float32(hints.GetLat()),
			Lon:       float32(hints.GetLon()),
			Precision: hints.GetPrecision(),
			Role:      dorv1.Role_ROLE_VENUE,
			Weight:    1,
		}}
		res.Backend = "hints"
		res.Fallback = true
		res.Degraded = true
		return res
	}

	candidates := p.candidates(article, countryHint, 8)
	if p.deps.Decider != nil && len(candidates) > 0 {
		ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		resp, err := p.deps.Decider.Decide(ctx, decide.PlotRequest(decideArticle(article), toDecideCandidates(candidates)))
		cancel()
		if err == nil {
			combined := decide.CombinePlot(resp, toDecideCandidates(candidates), p.thresholds())
			if combined.Accepted {
				return p.fromJev(ctx, article, candidates, combined)
			}
		} else {
			p.log.Warn("plot: jev unavailable; trying fallback", "article_id", article.Id, "err", err)
		}
	}
	if p.deps.Enricher != nil {
		if placed, ok := p.fromClaude(ctx, article, candidates, countryHint); ok {
			return placed
		}
	}
	if hints != nil && hints.GetLocated() && hints.GetLat() != 0 && hints.GetLon() != 0 {
		res.Locations = []*dorv1.Location{{
			PlaceId:   "hint:" + firstNonEmpty(hints.GetEventId(), article.GetId()),
			Name:      firstNonEmpty(hints.GetPlace(), article.GetDateline(), countryHint, "Hinted location"),
			Lat:       float32(hints.GetLat()),
			Lon:       float32(hints.GetLon()),
			Precision: hints.GetPrecision(),
			Role:      dorv1.Role_ROLE_ORIGIN,
			Weight:    0.8,
		}}
		res.Backend = "hints"
		res.Fallback = true
		res.Degraded = true
		return res
	}
	return p.deterministic(article, candidates, countryHint)
}

func (p *plotter) candidates(article *dorv1.Article, countryHint string, max int) []geo.Place {
	if p.deps.Gazetteer == nil || !p.deps.Gazetteer.Loaded() {
		return nil
	}
	title := article.GetTitle()
	if hints := article.GetHints(); hints != nil && hints.GetPlace() != "" {
		title += " " + hints.GetPlace()
	}
	return p.deps.Gazetteer.Candidates(article.GetLede(), title, article.GetDateline(), countryHint, max)
}

func (p *plotter) fromJev(ctx context.Context, article *dorv1.Article, candidates []geo.Place, result decide.PlotResult) *dorv1.PlacedArticle {
	if result.PrimaryIndex < 0 || result.PrimaryIndex >= len(candidates) {
		return p.deterministic(article, candidates, "")
	}
	primary := candidates[result.PrimaryIndex]
	prec := precisionFromString(result.Precision)
	if !result.PrecisionOK {
		prec = dorv1.Precision_PRECISION_CITY
	}
	role := dorv1.Role_ROLE_ORIGIN
	if datelineMatches(article.GetDateline(), primary) {
		role = dorv1.Role_ROLE_VENUE
	}
	loc := placeLocation(primary, prec, role, 1)
	if prec == dorv1.Precision_PRECISION_STREET || prec == dorv1.Precision_PRECISION_POINT {
		loc = p.geocodePrecise(ctx, article, primary, prec, role)
	}
	locations := []*dorv1.Location{loc}
	partyIDs := make(map[string]bool, len(result.Parties))
	for _, id := range result.Parties {
		partyIDs[id] = true
	}
	for _, c := range candidates {
		if c.ID == primary.ID || !partyIDs[c.ID] {
			continue
		}
		w := result.PartyP[c.ID]
		if w <= 0 {
			w = 0.7
		}
		locations = append(locations, placeLocation(c, dorv1.Precision_PRECISION_CITY, dorv1.Role_ROLE_PARTY, float32(w)))
	}
	return &dorv1.PlacedArticle{
		Article:     article,
		Locations:   capLocations(locations),
		Primary:     0,
		MultiRegion: multiCountry(candidates, locations),
		Fallback:    false,
		Degraded:    false,
		Backend:     "jev",
	}
}

func (p *plotter) fromClaude(ctx context.Context, article *dorv1.Article, candidates []geo.Place, countryHint string) (*dorv1.PlacedArticle, bool) {
	callCtx, cancel := context.WithTimeout(ctx, 1800*time.Millisecond)
	defer cancel()
	job := llm.EnricherJob(llm.Article{
		Title:    article.GetTitle(),
		Lede:     article.GetLede(),
		Dateline: article.GetDateline(),
		Source:   article.GetSourceId(),
	})
	res, err := p.deps.Enricher.Complete(callCtx, job)
	if err != nil {
		// A subscription CLI cannot answer inside this deadline and declines
		// before starting; that is the configured setup, not a failure.
		if !errors.Is(err, llm.ErrCLITooSlow) {
			p.log.Warn("plot: claude fallback failed", "article_id", article.Id, "err", err)
		}
		return nil, false
	}
	var enrichment llm.Enrichment
	if err := res.Decode(&enrichment); err != nil {
		p.log.Warn("plot: claude output invalid", "article_id", article.Id, "err", err)
		return nil, false
	}
	placeName := strings.TrimSpace(enrichment.PlaceName)
	country := strings.TrimSpace(enrichment.Country)
	if placeName == "" && country == "" {
		return nil, false
	}
	var chosen geo.Place
	var ok bool
	if placeName != "" && p.deps.Gazetteer != nil {
		chosen, ok = p.deps.Gazetteer.Lookup(placeName, country)
	}
	if !ok && country != "" && p.deps.Gazetteer != nil {
		chosen, ok = p.deps.Gazetteer.CountryCentroid(country)
	}
	if !ok && placeName != "" && p.deps.Photon != nil {
		places, gerr := p.deps.Photon.Geocode(callCtx, strings.TrimSpace(placeName+", "+country), 0, 0, 1)
		if gerr == nil && len(places) > 0 {
			chosen, ok = places[0], true
		}
	}
	if !ok {
		return nil, false
	}
	prec := dorv1.Precision_PRECISION_CITY
	if placeName == "" || strings.EqualFold(placeName, country) || chosen.CountryID {
		prec = dorv1.Precision_PRECISION_COUNTRY
	}
	role := dorv1.Role_ROLE_ORIGIN
	if datelineMatches(article.GetDateline(), chosen) {
		role = dorv1.Role_ROLE_VENUE
	}
	loc := placeLocation(chosen, prec, role, 1)
	return &dorv1.PlacedArticle{
		Article:   article,
		Locations: []*dorv1.Location{loc},
		Primary:   0,
		Fallback:  true,
		Degraded:  false,
		Backend:   "claude",
	}, true
}

func (p *plotter) deterministic(article *dorv1.Article, candidates []geo.Place, countryHint string) *dorv1.PlacedArticle {
	res := &dorv1.PlacedArticle{Article: article, Fallback: true, Degraded: true, Backend: "gazetteer"}
	if len(candidates) > 0 {
		// Only accept a deterministic candidate when the text itself is a
		// strong signal (title/dateline). Otherwise a country centroid is more
		// honest than a confident-looking city guess.
		c := candidates[0]
		if c.Score >= 10 || len(candidates) == 1 {
			prec := dorv1.Precision_PRECISION_CITY
			if c.CountryID {
				prec = dorv1.Precision_PRECISION_COUNTRY
			}
			role := dorv1.Role_ROLE_ORIGIN
			if datelineMatches(article.GetDateline(), c) {
				role = dorv1.Role_ROLE_VENUE
			}
			res.Locations = []*dorv1.Location{placeLocation(c, prec, role, 0.6)}
			return res
		}
	}
	if countryHint != "" && p.deps.Gazetteer != nil {
		if c, ok := p.deps.Gazetteer.CountryCentroid(countryHint); ok {
			res.Locations = []*dorv1.Location{placeLocation(c, dorv1.Precision_PRECISION_COUNTRY, dorv1.Role_ROLE_ORIGIN, 0.5)}
			return res
		}
	}
	if article.GetDateline() != "" && p.deps.Gazetteer != nil {
		if c, ok := p.deps.Gazetteer.Lookup(article.GetDateline(), countryHint); ok {
			res.Locations = []*dorv1.Location{placeLocation(c, dorv1.Precision_PRECISION_CITY, dorv1.Role_ROLE_VENUE, 0.5)}
			return res
		}
	}
	res.Backend = "none"
	return res
}

func (p *plotter) geocodePrecise(ctx context.Context, article *dorv1.Article, c geo.Place, prec dorv1.Precision, role dorv1.Role) *dorv1.Location {
	fallback := placeLocation(c, prec, role, 1)
	if p.deps.Photon == nil {
		return fallback
	}
	query := strings.TrimSpace(c.Name + ", " + c.Admin + ", " + c.Country)
	if query == ", , " || query == "" {
		query = firstNonEmpty(article.GetDateline(), article.GetTitle())
	}
	places, err := p.deps.Photon.Geocode(ctx, query, c.Lat, c.Lon, 1)
	if err != nil || len(places) == 0 {
		return fallback
	}
	g := places[0]
	return &dorv1.Location{
		PlaceId:   g.ID,
		Name:      g.Name,
		Lat:       float32(g.Lat),
		Lon:       float32(g.Lon),
		Precision: prec,
		Role:      role,
		Weight:    1,
	}
}

func (p *plotter) thresholds() config.Thresholds {
	t := p.deps.Thresholds
	if t.Classify.Accept == 0 && t.Plot.Primary == 0 && t.Relate.Accept == 0 {
		return decide.DefaultThresholds()
	}
	return t
}

func decideArticle(a *dorv1.Article) decide.Article {
	return decide.Article{
		ID:       a.GetId(),
		Title:    a.GetTitle(),
		Lede:     a.GetLede(),
		Dateline: a.GetDateline(),
		Source:   a.GetSourceId(),
	}
}

func toDecideCandidates(in []geo.Place) []decide.Candidate {
	out := make([]decide.Candidate, 0, len(in))
	for _, c := range in {
		out = append(out, decide.Candidate{
			ID: c.ID, Name: c.Name, Admin: c.Admin, Country: c.Country, Population: c.Population,
		})
	}
	return out
}

func placeLocation(c geo.Place, prec dorv1.Precision, role dorv1.Role, weight float32) *dorv1.Location {
	return &dorv1.Location{
		PlaceId:   c.ID,
		Name:      c.Name,
		Lat:       float32(c.Lat),
		Lon:       float32(c.Lon),
		Precision: prec,
		Role:      role,
		Weight:    weight,
	}
}

func precisionFromString(s string) dorv1.Precision {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "country":
		return dorv1.Precision_PRECISION_COUNTRY
	case "region":
		return dorv1.Precision_PRECISION_REGION
	case "street":
		return dorv1.Precision_PRECISION_STREET
	case "point":
		return dorv1.Precision_PRECISION_POINT
	default:
		return dorv1.Precision_PRECISION_CITY
	}
}

func datelineMatches(dateline string, c geo.Place) bool {
	d := normalizeWords(dateline)
	if d == "" {
		return false
	}
	n := normalizeWords(c.Name)
	if n == "" {
		return false
	}
	return d == n || strings.Contains(d, n) || strings.Contains(n, d)
}

func normalizeWords(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return r
		}
		return ' '
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

func capLocations(in []*dorv1.Location) []*dorv1.Location {
	if len(in) <= 6 {
		return in
	}
	return in[:6]
}

func multiCountry(candidates []geo.Place, locations []*dorv1.Location) bool {
	seen := make(map[string]bool, len(locations))
	for i, l := range locations {
		if i == 0 {
			continue
		}
		if l.Role != dorv1.Role_ROLE_PARTY {
			continue
		}
		for _, c := range candidates {
			if c.ID == l.PlaceId && c.CountryCode != "" {
				seen[c.CountryCode] = true
			}
		}
	}
	return len(seen) > 1
}
