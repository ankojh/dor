package agents

import (
	"strings"
	"unicode"
)

// firstNonEmpty returns the first trimmed non-empty string.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func titleJaccard(a, b string) float64 {
	sa := titleTokens(a)
	sb := titleTokens(b)
	if len(sa) == 0 || len(sb) == 0 {
		return 0
	}
	inter := 0
	for t := range sa {
		if sb[t] {
			inter++
		}
	}
	union := len(sa) + len(sb) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func titleTokens(s string) map[string]bool {
	out := make(map[string]bool)
	var cur []rune
	flush := func() {
		if len(cur) < 3 {
			cur = cur[:0]
			return
		}
		word := strings.ToLower(string(cur))
		switch word {
		case "the", "and", "for", "with", "from", "that", "this", "after",
			"before", "into", "over", "under", "says", "said", "will", "has",
			"have", "were", "was", "its", "his", "her", "their", "they":
		default:
			out[word] = true
		}
		cur = cur[:0]
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur = append(cur, r)
			continue
		}
		flush()
	}
	flush()
	return out
}

func entityJaccard(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	sa := make(map[string]bool, len(a))
	for _, e := range a {
		sa[strings.ToLower(strings.TrimSpace(e))] = true
	}
	inter := 0
	for _, e := range b {
		if sa[strings.ToLower(strings.TrimSpace(e))] {
			inter++
		}
	}
	return float64(inter) / float64(min(len(a), len(b)))
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[strings.ToLower(s)] {
			continue
		}
		seen[strings.ToLower(s)] = true
		out = append(out, s)
	}
	return out
}
