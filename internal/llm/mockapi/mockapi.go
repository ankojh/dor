// Package mockapi is an in-process fake Claude Messages endpoint.
//
// It returns realistic beta Messages responses — a structured-output text
// block, stop_reason, stop_details, and the prompt-cache usage fields — so the
// real SDK path is exercised without a key. Answers are generated from the
// request's JSON schema and are semantically meaningless: reports produced
// against mock backends are NOT measurements.
package mockapi

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankojh/dor/internal/llm"
)

// Options configures the fake endpoint.
type Options struct {
	Latency         time.Duration
	RefusalEvery    uint64
	MaxTokensEvery  uint64
	CacheSimulation bool
}

// DefaultOptions returns the eval defaults.
func DefaultOptions() Options {
	return Options{
		Latency:         2 * time.Millisecond,
		RefusalEvery:    0,
		MaxTokensEvery:  0,
		CacheSimulation: true,
	}
}

type request struct {
	Model     string `json:"model"`
	MaxTokens int64  `json:"max_tokens"`
	System    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"system"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	OutputConfig struct {
		Effort string `json:"effort"`
		Format struct {
			Type   string          `json:"type"`
			Schema json.RawMessage `json:"schema"`
		} `json:"format"`
	} `json:"output_config"`
}

// Server is the fake API plus its cache simulation.
type Server struct {
	opts  Options
	mu    sync.Mutex
	seen  map[uint64]struct{}
	count uint64
}

// NewHandler returns the fake Messages handler.
func NewHandler(opts Options) http.Handler {
	s := &Server{opts: opts, seen: make(map[uint64]struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", s.handle)
	return mux
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if s.opts.Latency > 0 {
		time.Sleep(s.opts.Latency)
	}
	var req request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON")
		return
	}
	s.mu.Lock()
	s.count++
	n := s.count
	s.mu.Unlock()

	var systemText strings.Builder
	for _, sys := range req.System {
		systemText.WriteString(sys.Text)
	}
	var userText strings.Builder
	for _, m := range req.Messages {
		userText.WriteString(blockText(m.Content))
	}
	prefix := systemText.String() + string(req.OutputConfig.Format.Schema)

	cacheCreate, cacheRead := int64(0), int64(0)
	if s.opts.CacheSimulation {
		key := fnv64([]byte(prefix))
		s.mu.Lock()
		_, hit := s.seen[key]
		if !hit {
			s.seen[key] = struct{}{}
		}
		s.mu.Unlock()
		tokens := estimate(prefix)
		if hit {
			cacheRead = tokens
		} else {
			cacheCreate = tokens
		}
	}

	seed := fnv64([]byte(req.Model + prefix + userText.String()))
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	if s.opts.RefusalEvery > 0 && n%s.opts.RefusalEvery == 0 {
		writeMessage(w, req.Model, nil, "refusal",
			map[string]any{"category": "general_harms", "explanation": "mock refusal"},
			estimate(userText.String()), cacheCreate, cacheRead)
		return
	}
	if s.opts.MaxTokensEvery > 0 && n%s.opts.MaxTokensEvery == 0 {
		writeMessage(w, req.Model, nil, "max_tokens", nil,
			estimate(userText.String()), cacheCreate, cacheRead)
		return
	}

	var schema map[string]any
	if len(req.OutputConfig.Format.Schema) > 0 {
		if err := json.Unmarshal(req.OutputConfig.Format.Schema, &schema); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid schema")
			return
		}
		if bad := llm.UnsupportedSchemaKeywords(schema); len(bad) > 0 {
			writeError(w, http.StatusBadRequest, "invalid_request_error",
				"unsupported schema keyword: "+strings.Join(bad, ", "))
			return
		}
		if err := llm.ValidateSchema(schema); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
			return
		}
	}
	out := map[string]any{}
	if props, ok := schema["properties"].(map[string]any); ok {
		for _, name := range sortedKeys(props) {
			prop, _ := props[name].(map[string]any)
			out[name] = generate(prop, rng, name)
		}
	}
	text, _ := json.Marshal(out)
	writeMessage(w, req.Model, text, "end_turn", nil,
		estimate(userText.String()), cacheCreate, cacheRead)
}

func writeMessage(w http.ResponseWriter, model string, text []byte, stop string, stopDetails map[string]any, input, cacheCreate, cacheRead int64) {
	content := []any{}
	if text != nil {
		content = append(content, map[string]any{"type": "text", "text": string(text)})
	}
	resp := map[string]any{
		"id":            "msg_mock_" + itoa(fnv64(text)),
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"content":       content,
		"stop_reason":   stop,
		"stop_sequence": nil,
		"usage": map[string]any{
			"input_tokens":                input,
			"output_tokens":               estimateBytes(text),
			"cache_creation_input_tokens": cacheCreate,
			"cache_read_input_tokens":     cacheRead,
			"cache_creation": map[string]any{
				"ephemeral_5m_input_tokens": cacheCreate,
				"ephemeral_1h_input_tokens": 0,
			},
			"output_tokens_details": map[string]any{"thinking_tokens": 0},
			"server_tool_use":       map[string]any{"web_search_requests": 0, "web_fetch_requests": 0},
			"service_tier":          "standard",
			"speed":                 "standard",
			"inference_geo":         "not_available",
			"iterations":            []any{},
		},
	}
	if stopDetails != nil {
		resp["stop_details"] = stopDetails
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func blockText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil {
		var b strings.Builder
		for _, bl := range blocks {
			b.WriteString(bl.Text)
		}
		return b.String()
	}
	return ""
}

func generate(schema map[string]any, rng *rand.Rand, field string) any {
	if schema == nil {
		return "mock"
	}
	if enum, ok := schema["enum"].([]any); ok && len(enum) > 0 {
		return enum[rng.IntN(len(enum))]
	}
	typ, _ := schema["type"].(string)
	switch typ {
	case "object":
		out := map[string]any{}
		props, _ := schema["properties"].(map[string]any)
		for _, name := range sortedKeys(props) {
			prop, _ := props[name].(map[string]any)
			out[name] = generate(prop, rng, name)
		}
		return out
	case "array":
		items, _ := schema["items"].(map[string]any)
		n := 2
		if min, ok := constraint(schema, "minItems", minItemsRe); ok && int(min) > n {
			n = int(min)
		}
		if max, ok := constraint(schema, "maxItems", maxItemsRe); ok && int(max) < n {
			n = int(max)
		}
		if n < 0 {
			n = 0
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, generate(items, rng, field))
		}
		return out
	case "integer":
		lo, hi := 0, 10
		if v, ok := constraint(schema, "minimum", minimumRe); ok {
			lo = int(v)
		}
		if v, ok := constraint(schema, "maximum", maximumRe); ok {
			hi = int(v)
		}
		if hi < lo {
			hi = lo
		}
		return lo + rng.IntN(hi-lo+1)
	case "number":
		lo, hi := 0.0, 1.0
		if v, ok := constraint(schema, "minimum", minimumRe); ok {
			lo = v
		}
		if v, ok := constraint(schema, "maximum", maximumRe); ok {
			hi = v
		}
		if hi < lo {
			hi = lo
		}
		return lo + rng.Float64()*(hi-lo)
	case "boolean":
		return rng.IntN(2) == 0
	case "string":
		out := mockString(field, rng)
		if max, ok := constraint(schema, "maxLength", maxLengthRe); ok && len(out) > int(max) {
			out = out[:int(max)]
		}
		return out
	default:
		if typ == "" {
			return "mock"
		}
		return "mock"
	}
}

// The client strips JSON-Schema constraints from the wire schema and restates
// them in the property description ("Maximum 5.", "At most 2 items."), so the
// mock reads them back from there; a literal keyword still wins.
var (
	minimumRe   = regexp.MustCompile(`Minimum (-?\d+(?:\.\d+)?)`)
	maximumRe   = regexp.MustCompile(`Maximum (-?\d+(?:\.\d+)?)`)
	minItemsRe  = regexp.MustCompile(`At least (\d+) items`)
	maxItemsRe  = regexp.MustCompile(`At most (\d+) items`)
	maxLengthRe = regexp.MustCompile(`At most (\d+) characters`)
)

func constraint(schema map[string]any, key string, re *regexp.Regexp) (float64, bool) {
	if v, ok := schema[key].(float64); ok {
		return v, true
	}
	desc, _ := schema["description"].(string)
	m := re.FindStringSubmatch(desc)
	if m == nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(m[1], 64)
	return v, err == nil
}

func mockString(field string, rng *rand.Rand) string {
	switch field {
	case "title":
		return fmt.Sprintf("Mock headline %d", rng.IntN(1000))
	case "brief":
		return "Mock brief. It exists only to exercise the structured-output path."
	case "why":
		return "Mock connection: the two stories share a cause."
	default:
		return fmt.Sprintf("mock %s", field)
	}
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    typ,
			"message": msg,
		},
	})
}

func estimate(s string) int64 { return int64(len(s)/4 + 1) }

func estimateBytes(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	return estimate(string(b))
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
