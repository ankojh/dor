// Package sources loads and validates the reviewed source seed list in
// deploy/sources/sources.yaml. M1·2 seeds the same file into Postgres; the
// fetchers read it directly at startup so local runs need no database.
package sources

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Kind is the fetch family of a source. The first five are v1; the rest are
// documented for later so the seed list stays the single registry.
type Kind string

// Source kinds. RSS covers RSS 2.0, Atom and RDF feeds.
const (
	KindRSS       Kind = "rss"
	KindGDELT     Kind = "gdelt"
	KindUSGS      Kind = "usgs"
	KindGDACS     Kind = "gdacs"
	KindEONET     Kind = "eonet"
	KindReliefWeb Kind = "reliefweb"
)

// AllKinds is the set of kinds the loader accepts.
var AllKinds = []Kind{KindRSS, KindGDELT, KindUSGS, KindGDACS, KindEONET, KindReliefWeb}

// Source is one reviewed source. The fields before terms_note mirror the
// plan's Postgres `sources` table (§6); region and hazard are extra fields the
// fetchers and the seed report use.
type Source struct {
	ID        string `yaml:"id"`
	Kind      Kind   `yaml:"kind"`
	URL       string `yaml:"url"`
	Country   string `yaml:"country"`
	Region    string `yaml:"region"`
	Lang      string `yaml:"lang"`
	IntervalS int    `yaml:"interval_s"`
	Enabled   bool   `yaml:"enabled"`
	TermsOK   bool   `yaml:"terms_ok"`
	TermsNote string `yaml:"terms_note"`
	// Hazard is the hazard family for hazard sources (earthquake, flood, ...).
	Hazard string `yaml:"hazard,omitempty"`
	// Notes records the live feed check and any other review note.
	Notes string `yaml:"notes,omitempty"`
}

// File is the YAML document.
type File struct {
	Version    int      `yaml:"version"`
	ReviewedAt string   `yaml:"reviewed_at"`
	Notes      string   `yaml:"notes"`
	Sources    []Source `yaml:"sources"`
}

// Interval returns the polling interval, with the per-kind default when the
// file does not set one.
func (s Source) Interval() time.Duration {
	if s.IntervalS > 0 {
		return time.Duration(s.IntervalS) * time.Second
	}
	switch s.Kind {
	case KindRSS:
		return 3 * time.Minute
	case KindGDELT:
		return 15 * time.Minute
	case KindReliefWeb:
		return time.Hour
	default:
		return 5 * time.Minute
	}
}

// Load reads and validates a source file.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("sources: read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse decodes and validates a source file.
func Parse(data []byte) (*File, error) {
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("sources: parse yaml: %w", err)
	}
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

// validID is the NATS KV key charset. Source ids are used directly as
// source_health keys, so an id with a space, "*", ">" or ":" would fail only at
// runtime; the loader rejects it up front.
var validID = regexp.MustCompile(`^[-/_=.a-zA-Z0-9]+$`)

// Validate enforces the review rules: unique ids, a known kind, an absolute
// https URL, a positive interval and, above all, that nothing is enabled
// without a terms review.
func (f *File) Validate() error {
	if f.Version != 1 {
		return fmt.Errorf("sources: unsupported version %d", f.Version)
	}
	if len(f.Sources) == 0 {
		return errors.New("sources: list is empty")
	}
	seen := make(map[string]bool, len(f.Sources))
	allowed := make(map[Kind]bool, len(AllKinds))
	for _, k := range AllKinds {
		allowed[k] = true
	}
	for i, s := range f.Sources {
		where := fmt.Sprintf("sources[%d] (%s)", i, s.ID)
		if strings.TrimSpace(s.ID) == "" {
			return fmt.Errorf("sources[%d]: id is required", i)
		}
		if seen[s.ID] {
			return fmt.Errorf("%s: duplicate id", where)
		}
		if !validID.MatchString(s.ID) {
			return fmt.Errorf("%s: id must match %s (it is also a NATS KV key)", where, validID)
		}
		seen[s.ID] = true
		if !allowed[s.Kind] {
			return fmt.Errorf("%s: unknown kind %q", where, s.Kind)
		}
		u, err := url.Parse(s.URL)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("%s: url must be an absolute https URL, got %q", where, s.URL)
		}
		if s.IntervalS < 30 || s.IntervalS > 86400 {
			return fmt.Errorf("%s: interval_s must be 30..86400, got %d", where, s.IntervalS)
		}
		if strings.TrimSpace(s.Country) == "" {
			return fmt.Errorf("%s: country is required", where)
		}
		if s.Kind == KindRSS {
			if s.Region == "" {
				return fmt.Errorf("%s: region is required for an rss source", where)
			}
			if s.Lang != "en" {
				return fmt.Errorf("%s: v1 ingests English feeds only, got lang %q", where, s.Lang)
			}
		}
		if s.Enabled && !s.TermsOK {
			return fmt.Errorf("%s: enabled source has terms_ok=false; review the terms first", where)
		}
		if s.Enabled && strings.TrimSpace(s.TermsNote) == "" {
			return fmt.Errorf("%s: enabled source needs a terms_note", where)
		}
	}
	return nil
}

// ForKind returns enabled, terms-cleared sources of one kind, in file order.
func (f *File) ForKind(kind Kind) []Source {
	out := make([]Source, 0, len(f.Sources))
	for _, s := range f.Sources {
		if s.Kind == kind && s.Enabled && s.TermsOK {
			out = append(out, s)
		}
	}
	return out
}

// All returns every source in file order, enabled or not.
func (f *File) All() []Source { return append([]Source(nil), f.Sources...) }

// Counts summarises the file for logs and the acceptance report.
type Counts struct {
	Total     int
	Enabled   int
	ByRegion  map[string]int
	ByKind    map[string]int
	Disabled  int
	ReviewDue int // enabled with terms_ok but no explicit note of a human review
}

// Count returns the summary counts.
func (f *File) Count() Counts {
	c := Counts{Total: len(f.Sources), ByRegion: map[string]int{}, ByKind: map[string]int{}}
	for _, s := range f.Sources {
		c.ByKind[string(s.Kind)]++
		if !s.Enabled {
			c.Disabled++
			continue
		}
		c.Enabled++
		if s.Kind == KindRSS {
			c.ByRegion[s.Region]++
		}
	}
	return c
}
