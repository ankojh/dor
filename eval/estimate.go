package eval

import (
	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/decide"
	"github.com/ankojh/dor/internal/llm"
)

// EstimateRun estimates live cost from the actual request and job builders, so
// a paid run is checked against eval_usd before the first call. It counts only
// items that have labels. jevPrice is the price of the jev host that will run
// live (OpenRouter or TypeSafe); a mock jev backend passes the zero Price.
func EstimateRun(ds *Dataset, models config.Models, jevPrice config.Price) (jevUSD, claudeUSD float64, calls int) {
	claudePrice := models.Claude.Price[models.Claude.HaikuModel]
	addJev := func(req decide.Request) {
		jevUSD += jevPrice.Cost(decide.EstimateRequestTokens(req), 0)
		calls++
	}
	addClaude := func(job llm.Job) {
		in, out := llm.EstimateJobTokens(job)
		claudeUSD += claudePrice.Cost(in, out)
		calls++
	}
	for _, a := range ds.Articles {
		if _, ok := ds.ArticleLabels[a.ID]; !ok {
			continue
		}
		addJev(decide.ClassifyRequest(decideArticle(a)))
		addJev(decide.PlotRequest(decideArticle(a), decideCandidates(a.Candidates)))
		addClaude(claudeClassifyJob(a))
		job, _ := claudePlotJob(a)
		addClaude(job)
	}
	for _, p := range ds.Pairs {
		if _, ok := ds.PairLabels[p.ID]; !ok {
			continue
		}
		a, okA := findArticle(ds.Articles, p.A)
		b, okB := findArticle(ds.Articles, p.B)
		if !okA || !okB {
			continue
		}
		addJev(decide.RelateRequest(decideStory(a), decideStory(b)))
		addClaude(claudeRelateJob(a, b))
	}
	for _, m := range ds.Matches {
		a, ok := findArticle(ds.Articles, m.ArticleID)
		if !ok {
			continue
		}
		addJev(decide.MatchRequest(decideStory(a), m.Prompt))
		addClaude(claudeMatchJob(m, a))
	}
	return jevUSD, claudeUSD, calls
}
