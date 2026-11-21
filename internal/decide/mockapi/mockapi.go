// Package mockapi is an in-process fake jev decisions endpoint.
//
// It exists so `make eval` and the client tests can exercise the real HTTP
// path — JSON encoding, retry classes, response parsing, fail-closed arms —
// without a key. Answers are deterministic per request body but semantically
// meaningless: a report produced against mock backends is NOT a measurement
// and says so on its first line.
package mockapi

import (
	"encoding/json"
	"net/http"
	"time"

	rand "math/rand/v2"
)

// Options configures the fake endpoint.
type Options struct {
	// Latency is slept per request. Keep it tiny; it is a mock.
	Latency time.Duration
	// InputPerMTok prices the fake input tokens in usage.cost.
	InputPerMTok float64
	// RefusalEvery turns one answer into a refusal every N requests (0 off).
	RefusalEvery uint64
	// MissingEvery omits one answer every N requests (0 off).
	MissingEvery uint64
	// UnknownEvery returns an unknown answer type every N requests (0 off).
	UnknownEvery uint64
	// ServedSuffix is appended to the model id to mimic a dated snapshot.
	ServedSuffix string
}

// DefaultOptions returns the eval defaults.
func DefaultOptions() Options {
	return Options{
		Latency:      time.Millisecond,
		InputPerMTok: 0.10,
		RefusalEvery: 97,
		MissingEvery: 89,
		UnknownEvery: 263,
		ServedSuffix: "-20261014",
	}
}

type request struct {
	Model     string                     `json:"model"`
	State     json.RawMessage            `json:"state"`
	Questions map[string]json.RawMessage `json:"questions"`
	SessionID string                     `json:"session_id"`
	User      string                     `json:"user"`
}

type question struct {
	Type         string                     `json:"type"`
	Instructions string                     `json:"instructions"`
	Criteria     json.RawMessage            `json:"criteria"`
	raw          map[string]json.RawMessage // filled when criteria is an object
}

type answer struct {
	Type          string             `json:"type"`
	Noul          *float64           `json:"noul,omitempty"`
	Choice        *string            `json:"choice,omitempty"`
	Score         *float64           `json:"score,omitempty"`
	Confidence    *float64           `json:"confidence,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Legend        map[string]string  `json:"legend,omitempty"`
}

// NewHandler returns the fake decisions handler.
func NewHandler(opts Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/alpha/decisions", func(w http.ResponseWriter, r *http.Request) {
		if opts.Latency > 0 {
			time.Sleep(opts.Latency)
		}
		var req request
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, 400, "invalid JSON")
			return
		}
		body, _ := json.Marshal(req)
		seed := fnv64(body)

		answers := make(map[string]answer, len(req.Questions))
		keys := make([]string, 0, len(req.Questions))
		for k := range req.Questions {
			keys = append(keys, k)
		}
		// Sort for deterministic behaviour regardless of map order.
		sortStrings(keys)
		for i, key := range keys {
			var q question
			_ = json.Unmarshal(req.Questions[key], &q)
			if len(q.Criteria) > 0 {
				_ = json.Unmarshal(q.Criteria, &q.raw)
			}
			if opts.UnknownEvery > 0 && (seed+uint64(i))%opts.UnknownEvery == 0 {
				answers[key] = answer{Type: "mystery"}
				continue
			}
			if opts.MissingEvery > 0 && (seed+uint64(i))%opts.MissingEvery == 0 {
				continue
			}
			if opts.RefusalEvery > 0 && (seed+uint64(i))%opts.RefusalEvery == 0 {
				answers[key] = answer{Type: "refusal"}
				continue
			}
			answers[key] = answerFor(q, seed+uint64(i)*7919)
		}

		inputTokens := int64(len(body)/4 + 1)
		cost := float64(inputTokens) / 1e6 * opts.InputPerMTok
		resp := map[string]any{
			"id":       "gen-dec-mock-" + itoa(seed),
			"model":    req.Model + opts.ServedSuffix,
			"provider": "TypeSafe",
			"answers":  answers,
			"usage": map[string]any{
				"input_tokens":         inputTokens,
				"output_tokens":        10 * len(answers),
				"cost":                 cost,
				"input_tokens_details": map[string]any{"cached_tokens": 0},
				"cost_details":         map[string]any{"upstream_inference_prompt_cost": 0, "upstream_inference_completions_cost": 0},
				"is_byok":              false,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return mux
}

func answerFor(q question, seed uint64) answer {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	switch q.Type {
	case "noul":
		p := 0.05 + 0.9*rng.Float64()
		// Confidence is a normalized concentration, deliberately different
		// from P so a gate that uses it would produce different numbers.
		conf := 0.4 + 0.55*rng.Float64()
		return answer{Type: "noul", Noul: &p, Confidence: &conf}
	case "choice":
		opts := sortedKeys(q.raw)
		if len(opts) == 0 {
			opts = []string{"none"}
		}
		choice := opts[rng.IntN(len(opts))]
		chosen := 0.35 + 0.6*rng.Float64()
		probs := distribute(opts, choice, chosen)
		conf := 0.4 + 0.55*rng.Float64()
		return answer{Type: "choice", Choice: &choice, Confidence: &conf, Probabilities: probs}
	case "score":
		var levels []string
		if len(q.Criteria) > 0 {
			_ = json.Unmarshal(q.Criteria, &levels)
		}
		n := len(levels)
		if n == 0 {
			n = 1
		}
		idx := rng.IntN(n)
		chosen := 0.4 + 0.5*rng.Float64()
		probs := make(map[string]float64, n)
		rest := (1 - chosen) / float64(maxInt(n-1, 1))
		var score float64
		for i := 0; i < n; i++ {
			p := rest
			if i == idx {
				p = chosen
			}
			probs[itoa(uint64(i))] = p
			score += float64(i) * p
		}
		conf := 0.4 + 0.55*rng.Float64()
		return answer{Type: "score", Score: &score, Confidence: &conf, Probabilities: probs}
	default:
		return answer{Type: "mystery"}
	}
}

func distribute(opts []string, chosen string, p float64) map[string]float64 {
	out := make(map[string]float64, len(opts))
	rest := 0.0
	if len(opts) > 1 {
		rest = (1 - p) / float64(len(opts)-1)
	}
	for _, o := range opts {
		if o == chosen {
			out[o] = p
		} else {
			out[o] = rest
		}
	}
	return out
}

func writeError(w http.ResponseWriter, status, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": msg},
	})
}

func fnv64(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func sortedKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sortStrings(out)
	return out
}
