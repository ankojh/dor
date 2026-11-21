package gazetteer

import (
	"reflect"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"":                   "",
		"  Hello,  World!  ": "hello world",
		"São Paulo":          "são paulo",
		"St. John's":         "st john s",
		"New-York/NJ":        "new york nj",
		"2026 Election":      "2026 election",
		"!!!":                "",
		"Ürümqi\tChina\n":    "ürümqi china",
	}
	for in, want := range cases {
		if got := normalize(in); got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTokenize(t *testing.T) {
	got := tokenize("Iran's President met Putin, in St. Petersburg — 2026!")
	var raws, norms []string
	for _, tk := range got {
		raws = append(raws, tk.raw)
		norms = append(norms, tk.norm)
	}
	wantRaw := []string{"Iran's", "President", "met", "Putin", "in", "St", "Petersburg", "2026"}
	wantNorm := []string{"iran s", "president", "met", "putin", "in", "st", "petersburg", "2026"}
	if !reflect.DeepEqual(raws, wantRaw) {
		t.Errorf("raw = %q", raws)
	}
	if !reflect.DeepEqual(norms, wantNorm) {
		t.Errorf("norm = %q", norms)
	}
	if len(tokenize("")) != 0 || len(tokenize("  ,.; —")) != 0 {
		t.Error("punctuation-only input yields no tokens")
	}
}

func TestTokenizeKeepsApostropheInWord(t *testing.T) {
	got := tokenize("O’Brien")
	if len(got) != 1 || got[0].raw != "O’Brien" || got[0].norm != "o brien" {
		t.Errorf("tokens = %+v", got)
	}
}

func TestCapitalized(t *testing.T) {
	cases := map[string]bool{
		"Paris": true, "paris": false, "PARIS": true, "": false, "2026": false, "'Tis": true,
		"éclair": false, "Éclair": true, "9Paris": false, "-Lyon": true, "...": false,
	}
	for in, want := range cases {
		if got := capitalized(in); got != want {
			t.Errorf("capitalized(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestAmbiguousNames(t *testing.T) {
	m := ambiguousNames()
	for _, n := range []string{"mobile", "nice", "reading", "union", "government", "park"} {
		if _, ok := m[n]; !ok {
			t.Errorf("%q should be ambiguous", n)
		}
	}
	// Capital cities are taken literally in news.
	for _, n := range []string{"paris", "london", "berlin", "rome", "athens", "victoria"} {
		if _, ok := m[n]; ok {
			t.Errorf("%q must not be skipped", n)
		}
	}
}
