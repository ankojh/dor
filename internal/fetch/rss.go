package fetch

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"regexp"
	"strings"
	"time"

	xhtml "golang.org/x/net/html"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
)

// ParsedFeed is one decoded RSS 2.0, RSS 1.0 (RDF) or Atom feed.
type ParsedFeed struct {
	Title string
	Lang  string
	Items []FeedItem
}

// FeedItem is one entry of a feed, with short fields only.
type FeedItem struct {
	Title     string
	Link      string
	Summary   string
	Published time.Time
	GUID      string
	Lang      string
}

type feedDoc struct {
	XMLName xml.Name
	Channel struct {
		Title    string      `xml:"title"`
		Language string      `xml:"language"`
		Items    []feedEntry `xml:"item"`
	} `xml:"channel"`
	Items   []feedEntry `xml:"item"` // RSS 1.0 / RDF: items are siblings of channel
	Entries []feedEntry `xml:"entry"`
	Title   string      `xml:"title"`
	Lang    string      `xml:"lang,attr"`
}

type feedEntry struct {
	Title       string     `xml:"title"`
	Links       []feedLink `xml:"link"`
	Description string     `xml:"description"`
	Summary     string     `xml:"summary"`
	Content     string     `xml:"content"`
	Encoded     string     `xml:"encoded"` // content:encoded
	PubDate     string     `xml:"pubDate"`
	Published   string     `xml:"published"`
	Updated     string     `xml:"updated"`
	Date        string     `xml:"date"` // dc:date
	GUID        string     `xml:"guid"`
	ID          string     `xml:"id"`
	About       string     `xml:"about,attr"` // rdf:about in RSS 1.0
	Lang        string     `xml:"lang,attr"`
}

type feedLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
	Text string `xml:",chardata"`
}

func (l feedLink) url() string {
	if l.Href != "" {
		return strings.TrimSpace(l.Href)
	}
	return strings.TrimSpace(l.Text)
}

// ParseFeed decodes an RSS 2.0, RSS 1.0/RDF or Atom document. baseURL resolves
// relative item links. A document with an unknown root is an error, so an HTML
// error page is never mistaken for an empty feed.
func ParseFeed(r io.Reader, baseURL string) (*ParsedFeed, error) {
	data, err := io.ReadAll(io.LimitReader(r, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("fetch: read feed: %w", err)
	}
	data, err = normalizeEncoding(data)
	if err != nil {
		return nil, fmt.Errorf("fetch: decode feed: %w", err)
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	// Real-world feeds are not always well-formed XML: bare ampersands,
	// unquoted attributes and HTML entities are common in regional feeds. Be
	// lenient, but keep the root check below so an HTML error page is still
	// rejected as "not a feed".
	//
	// AutoClose is deliberately NOT xml.HTMLAutoClose: that list treats <link>
	// and <source> as void HTML elements, which auto-closes RSS's
	// <link>url</link> and drops every RSS 2.0 item. Strict=false plus the
	// HTML entity table covers the actual breakage; unescaped HTML in a
	// description is handled by the snippet tokenizer after decoding.
	dec.Strict = false
	dec.Entity = xml.HTMLEntity
	dec.CharsetReader = charsetReader
	var doc feedDoc
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("fetch: parse feed: %w", err)
	}
	switch strings.ToLower(doc.XMLName.Local) {
	case "rss", "rdf", "feed":
	default:
		return nil, fmt.Errorf("fetch: not a feed (root <%s>)", doc.XMLName.Local)
	}

	out := &ParsedFeed{Title: strings.TrimSpace(doc.Channel.Title), Lang: cleanLang(doc.Channel.Language)}
	if out.Title == "" {
		out.Title = strings.TrimSpace(doc.Title)
	}
	if out.Lang == "" {
		out.Lang = cleanLang(doc.Lang)
	}
	entries := doc.Channel.Items
	entries = append(entries, doc.Items...)
	entries = append(entries, doc.Entries...)
	for _, e := range entries {
		item := feedItem(e, baseURL)
		if item.Link == "" {
			continue
		}
		if item.Lang == "" {
			item.Lang = out.Lang
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}

func feedItem(e feedEntry, baseURL string) FeedItem {
	item := FeedItem{
		Title:   cleanText(e.Title, 300),
		Summary: cleanText(firstNonEmpty(e.Summary, e.Description, e.Content, e.Encoded), 400),
		GUID:    strings.TrimSpace(firstNonEmpty(e.GUID, e.ID, e.About)),
		Lang:    cleanLang(e.Lang),
	}
	for _, link := range e.Links {
		if link.Rel != "" && link.Rel != "alternate" {
			continue
		}
		if u := resolveURL(baseURL, link.url()); u != "" {
			item.Link = u
			break
		}
	}
	if item.Link == "" {
		if u := resolveURL(baseURL, item.GUID); u != "" {
			item.Link = u
		}
	}
	if item.Title == "" {
		item.Title = TitleFromURL(item.Link)
	}
	item.Published = firstTime(e.Published, e.PubDate, e.Updated, e.Date)
	return item
}

var whitespaceRuns = regexp.MustCompile(`\s+`)

// cleanText turns a feed field into a plain-text snippet and truncates it.
// Feeds routinely put HTML in titles and whole article bodies in description;
// the x/net/html tokenizer keeps the text and drops tags, so no markup leaks
// into the wire. Only a short snippet is kept; the full text is never stored.
func cleanText(s string, max int) string {
	if s == "" {
		return ""
	}
	s = htmlToText(s)
	s = whitespaceRuns.ReplaceAllString(strings.TrimSpace(s), " ")
	return truncateRunes(s, max)
}

// htmlToText renders a possibly-HTML fragment as text.
func htmlToText(s string) string {
	if !strings.Contains(s, "<") {
		return html.UnescapeString(s)
	}
	z := xhtml.NewTokenizer(strings.NewReader(s))
	var b strings.Builder
	for {
		switch z.Next() {
		case xhtml.ErrorToken:
			return html.UnescapeString(strings.TrimSpace(b.String()))
		case xhtml.TextToken:
			b.Write(z.Text())
			b.WriteByte(' ')
		case xhtml.StartTagToken, xhtml.EndTagToken, xhtml.SelfClosingTagToken:
			name, _ := z.TagName()
			switch string(name) {
			case "p", "br", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote":
				b.WriteByte(' ')
			}
		}
	}
}

func cleanLang(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if i := strings.IndexAny(s, "-_"); i > 0 {
		s = s[:i]
	}
	if len(s) < 2 || len(s) > 8 {
		return ""
	}
	for _, r := range s {
		if r < 'a' || r > 'z' {
			return ""
		}
	}
	return s
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

var feedTimeLayouts = []string{
	time.RFC1123Z,
	time.RFC1123,
	time.RFC822Z,
	time.RFC822,
	time.RFC3339Nano,
	time.RFC3339,
	"Mon, 2 Jan 2006 15:04:05 -0700",
	"Mon, 2 Jan 2006 15:04:05 MST",
	"Mon, 2 Jan 2006 15:04:05",
	"2006-01-02T15:04:05-0700",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"02 Jan 2006 15:04:05 -0700",
}

func firstTime(values ...string) time.Time {
	for _, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		for _, layout := range feedTimeLayouts {
			if t, err := time.Parse(layout, raw); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Time{}
}

// resolveURL makes a possibly relative link absolute. It returns "" for an
// empty or non-http value.
func resolveURL(baseURL, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	base, err := parseBaseURL(baseURL)
	if err != nil {
		return ""
	}
	// Protocol-relative links ("//host/path") inherit the base scheme.
	if strings.HasPrefix(ref, "//") {
		return base.scheme + ":" + ref
	}
	if !strings.HasPrefix(ref, "/") {
		return ""
	}
	return base.scheme + "://" + base.host + ref
}

type simpleURL struct{ scheme, host string }

func parseBaseURL(raw string) (simpleURL, error) {
	raw = strings.TrimSpace(raw)
	i := strings.Index(raw, "://")
	if i <= 0 {
		return simpleURL{}, fmt.Errorf("fetch: bad base url %q", raw)
	}
	scheme := raw[:i]
	rest := raw[i+3:]
	if j := strings.IndexAny(rest, "/?#"); j >= 0 {
		rest = rest[:j]
	}
	if rest == "" {
		return simpleURL{}, fmt.Errorf("fetch: bad base url %q", raw)
	}
	return simpleURL{scheme: scheme, host: rest}, nil
}

// encodingDecl matches the encoding pseudo-attribute of an XML declaration.
var encodingDecl = regexp.MustCompile(`(?i)^(<\?xml[^>]*?)\s+encoding="[^"]*"`)

// normalizeEncoding transcodes a UTF-16 feed (identified by its BOM) to UTF-8
// and drops the now-wrong encoding declaration. encoding/xml cannot read a
// UTF-16 declaration or an encoding name it has no decoder for, so this has to
// happen before the decoder sees the bytes.
func normalizeEncoding(data []byte) ([]byte, error) {
	if len(data) < 2 {
		return data, nil
	}
	var order unicode.Endianness
	var body []byte
	switch {
	case data[0] == 0xFF && data[1] == 0xFE:
		order, body = unicode.LittleEndian, data[2:]
	case data[0] == 0xFE && data[1] == 0xFF:
		order, body = unicode.BigEndian, data[2:]
	case data[0] == '<' && data[1] == 0:
		// UTF-16LE without a BOM: "<?xml" encodes as 3c 00 3f 00 ...
		order, body = unicode.LittleEndian, data
	case data[0] == 0 && data[1] == '<':
		order, body = unicode.BigEndian, data
	default:
		return data, nil
	}
	decoded, err := unicode.UTF16(order, unicode.IgnoreBOM).NewDecoder().Bytes(body)
	if err != nil {
		return nil, err
	}
	decoded = bytes.TrimPrefix(decoded, []byte("\xef\xbb\xbf"))
	return encodingDecl.ReplaceAll(decoded, []byte("$1")), nil
}

// charsetReader lets encoding/xml decode the legacy encodings that some feeds
// still declare. Unknown labels fall through unchanged (usually UTF-8).
func charsetReader(charset string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(charset)) {
	case "utf-8", "utf8", "us-ascii", "ascii", "":
		return input, nil
	case "utf-16", "utf16":
		return unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder().Reader(input), nil
	case "utf-16le", "utf16le":
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder().Reader(input), nil
	case "utf-16be", "utf16be":
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder().Reader(input), nil
	case "iso-8859-1", "latin1", "latin-1":
		return charmap.ISO8859_1.NewDecoder().Reader(input), nil
	case "windows-1252", "cp1252":
		return charmap.Windows1252.NewDecoder().Reader(input), nil
	default:
		data, err := io.ReadAll(io.LimitReader(input, 8<<20))
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(data), nil
	}
}
