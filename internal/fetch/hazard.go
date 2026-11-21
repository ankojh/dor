package fetch

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

// HazardItem is one alert from USGS, GDACS, NASA EONET or ReliefWeb.
type HazardItem struct {
	ID            string // stable upstream event id
	Revision      string // changes when the event escalates or is revised
	URL           string
	Title         string
	Summary       string
	Published     time.Time
	Updated       time.Time
	Lat           float64
	Lon           float64
	Located       bool
	Precision     PlacePrecision
	Hazard        string
	Country       string
	Magnitude     float64
	MagnitudeUnit string  // "Mw", "m/s", "knots", "acres", ...
	Severity      float64 // normalised 0..1 where the upstream provides a scale
	Place         string  // upstream place name
	Themes        []string
}

// usgsAlertSeverity maps the USGS PAGER alert level.
func usgsAlertSeverity(level string) (float64, bool) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "green":
		return 0.2, true
	case "yellow":
		return 0.5, true
	case "orange":
		return 0.8, true
	case "red":
		return 1, true
	default:
		return 0, false
	}
}

// ParseUSGS decodes a USGS GeoJSON earthquake feed.
func ParseUSGS(r io.Reader) ([]HazardItem, error) {
	var doc struct {
		Features []struct {
			ID         string `json:"id"`
			Properties struct {
				Mag      *float64 `json:"mag"`
				MagType  string   `json:"magType"`
				Place    string   `json:"place"`
				Time     int64    `json:"time"`
				Updated  int64    `json:"updated"`
				URL      string   `json:"url"`
				Title    string   `json:"title"`
				Type     string   `json:"type"`
				Alert    string   `json:"alert"`
				Signific *int     `json:"significance"`
			} `json:"properties"`
			Geometry struct {
				Type        string    `json:"type"`
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("fetch: parse usgs geojson: %w", err)
	}
	out := make([]HazardItem, 0, len(doc.Features))
	for _, f := range doc.Features {
		item := HazardItem{
			ID:        f.ID,
			Revision:  revisionString(f.Properties.Updated, f.Properties.Time),
			URL:       f.Properties.URL,
			Title:     cleanText(f.Properties.Title, 300),
			Hazard:    "earthquake",
			Place:     f.Properties.Place,
			Themes:    []string{"EARTHQUAKE"},
			Updated:   unixMillis(f.Properties.Updated),
			Published: unixMillis(f.Properties.Time),
			Precision: PrecisionPoint,
		}
		if len(f.Geometry.Coordinates) >= 2 {
			lon, lat := f.Geometry.Coordinates[0], f.Geometry.Coordinates[1]
			if validCoords(lat, lon) {
				item.Lat, item.Lon, item.Located = lat, lon, true
			}
		}
		if f.Properties.Mag != nil {
			item.Magnitude = *f.Properties.Mag
			item.MagnitudeUnit = f.Properties.MagType
		}
		if sev, ok := usgsAlertSeverity(f.Properties.Alert); ok {
			item.Severity = sev
		} else if f.Properties.Mag != nil {
			item.Severity = clamp01((*f.Properties.Mag - 2) / 6)
		}
		var summary []string
		if f.Properties.Place != "" {
			summary = append(summary, f.Properties.Place)
		}
		if f.Properties.Mag != nil {
			summary = append(summary, fmt.Sprintf("M%.1f", *f.Properties.Mag))
		}
		if f.Properties.Title == "" {
			item.Title = truncateRunes(strings.Join(summary, " · "), 300)
		}
		item.Summary = truncateRunes(strings.Join(summary, " · "), 400)
		out = append(out, item)
	}
	return out, nil
}

type gdacsDoc struct {
	Channel struct {
		Items []gdacsItem `xml:"item"`
	} `xml:"channel"`
}

type gdacsItem struct {
	Title       string `xml:"title"`
	Description string `xml:"description"`
	Link        string `xml:"link"`
	PubDate     string `xml:"pubDate"`
	FromDate    string `xml:"fromdate"`
	DateAdded   string `xml:"dateadded"`
	Point       string `xml:"point"` // georss:point, "lat lon"
	EventType   string `xml:"eventtype"`
	AlertLevel  string `xml:"alertlevel"`
	AlertScore  string `xml:"alertscore"`
	EventName   string `xml:"eventname"`
	EventID     string `xml:"eventid"`
	EpisodeID   string `xml:"episodeid"`
	Country     string `xml:"country"`
	ISO3        string `xml:"iso3"`
	Severity    struct {
		Unit  string  `xml:"unit,attr"`
		Value float64 `xml:"value,attr"`
		Text  string  `xml:",chardata"`
	} `xml:"severity"`
}

// ParseGDACS decodes the GDACS RSS feed (rss_7d.xml or rss.xml).
func ParseGDACS(r io.Reader) ([]HazardItem, error) {
	dec := xml.NewDecoder(r)
	dec.CharsetReader = charsetReader
	var doc gdacsDoc
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("fetch: parse gdacs rss: %w", err)
	}
	out := make([]HazardItem, 0, len(doc.Channel.Items))
	for _, it := range doc.Channel.Items {
		hazard := gdacsHazard(it.EventType)
		item := HazardItem{
			ID:            firstNonEmpty(it.EventID, it.EpisodeID),
			Revision:      strings.TrimSpace(it.EpisodeID) + ":" + strings.ToLower(strings.TrimSpace(it.AlertLevel)),
			URL:           strings.TrimSpace(it.Link),
			Title:         cleanText(it.Title, 300),
			Summary:       cleanText(it.Description, 400),
			Hazard:        hazard,
			Magnitude:     it.Severity.Value,
			MagnitudeUnit: it.Severity.Unit,
			Themes:        gdacsThemes(it.EventType, it.AlertLevel),
			Published:     firstTime(it.PubDate, it.FromDate, it.DateAdded),
			Precision:     PrecisionPoint,
		}
		// One consistent scale: the alert-level mapping is a floor, and the
		// more granular alert score can raise it.
		item.Severity = gdacsAlertSeverity(it.AlertLevel)
		if score, err := strconv.ParseFloat(strings.TrimSpace(it.AlertScore), 64); err == nil {
			if normalized := clamp01(score / 3); normalized > item.Severity {
				item.Severity = normalized
			}
		}
		if lat, lon, ok := parseGeoRSSPoint(it.Point); ok {
			item.Lat, item.Lon, item.Located = lat, lon, true
		}
		item.Country = strings.TrimSpace(it.ISO3)
		item.Place = strings.TrimSpace(it.Country)
		out = append(out, item)
	}
	return out, nil
}

func gdacsHazard(eventType string) string {
	switch strings.ToUpper(strings.TrimSpace(eventType)) {
	case "EQ":
		return "earthquake"
	case "TC":
		return "cyclone"
	case "FL":
		return "flood"
	case "VO":
		return "volcano"
	case "WF":
		return "wildfire"
	case "DR":
		return "drought"
	case "TS":
		return "tsunami"
	default:
		return strings.ToLower(strings.TrimSpace(eventType))
	}
}

func gdacsAlertSeverity(level string) float64 {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "green":
		return 0.2
	case "orange":
		return 0.7
	case "red":
		return 1
	default:
		return 0
	}
}

func gdacsThemes(eventType, alertLevel string) []string {
	var themes []string
	if eventType != "" {
		themes = append(themes, "GDACS_"+strings.ToUpper(eventType))
	}
	if alertLevel != "" {
		themes = append(themes, "ALERT_"+strings.ToUpper(alertLevel))
	}
	return themes
}

func parseGeoRSSPoint(raw string) (lat, lon float64, ok bool) {
	fields := strings.Fields(raw)
	if len(fields) < 2 {
		return 0, 0, false
	}
	la, err1 := strconv.ParseFloat(fields[0], 64)
	lo, err2 := strconv.ParseFloat(fields[1], 64)
	if err1 != nil || err2 != nil || !validCoords(la, lo) {
		return 0, 0, false
	}
	return la, lo, true
}

type eonetDoc struct {
	Events []struct {
		ID          string `json:"id"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Link        string `json:"link"`
		Closed      string `json:"closed"`
		Categories  []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		} `json:"categories"`
		Sources []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"sources"`
		Geometry []struct {
			MagnitudeValue *float64  `json:"magnitudeValue"`
			MagnitudeUnit  string    `json:"magnitudeUnit"`
			Date           string    `json:"date"`
			Type           string    `json:"type"`
			Coordinates    []float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"events"`
}

// ParseEONET decodes the NASA EONET v3 events API.
func ParseEONET(r io.Reader) ([]HazardItem, error) {
	var doc eonetDoc
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("fetch: parse eonet json: %w", err)
	}
	out := make([]HazardItem, 0, len(doc.Events))
	for _, e := range doc.Events {
		item := HazardItem{
			ID:        e.ID,
			Title:     cleanText(e.Title, 300),
			Summary:   cleanText(e.Description, 400),
			URL:       e.Link,
			Precision: PrecisionPoint,
		}
		var categoryID string
		var latestDate string
		for _, c := range e.Categories {
			if categoryID == "" {
				categoryID = c.ID
			}
			item.Themes = append(item.Themes, "EONET_"+strings.ToUpper(c.ID))
		}
		item.Hazard = eonetHazard(categoryID)
		if item.URL == "" && len(e.Sources) > 0 {
			item.URL = e.Sources[0].URL
		}
		// The latest point is the event's current position.
		var latest time.Time
		for _, g := range e.Geometry {
			if len(g.Coordinates) < 2 {
				continue
			}
			lon, lat := g.Coordinates[0], g.Coordinates[1]
			if !validCoords(lat, lon) {
				continue
			}
			at := firstTime(g.Date)
			if !item.Located || at.After(latest) {
				item.Lat, item.Lon, item.Located = lat, lon, true
				latest = at
				latestDate = g.Date
				item.Published = at
			}
			if g.MagnitudeValue != nil {
				item.Magnitude = *g.MagnitudeValue
				item.MagnitudeUnit = g.MagnitudeUnit
				item.Severity = eonetSeverity(categoryID, *g.MagnitudeValue, g.MagnitudeUnit)
			}
		}
		item.Revision = latestDate
		if e.Closed != "" {
			item.Themes = append(item.Themes, "CLOSED")
		}
		out = append(out, item)
	}
	return out, nil
}

func eonetHazard(categoryID string) string {
	switch strings.ToLower(categoryID) {
	case "wildfires":
		return "wildfire"
	case "severeStorms", "severestorms":
		return "storm"
	case "volcanoes":
		return "volcano"
	case "earthquakes":
		return "earthquake"
	case "floods":
		return "flood"
	case "drought":
		return "drought"
	case "sealakeice":
		return "sea_lake_ice"
	case "temperatureextremes":
		return "temperature_extreme"
	case "dusthaze":
		return "dust_haze"
	case "snow":
		return "snow"
	case "manmade":
		return "manmade"
	case "watercolor":
		return "water_color"
	default:
		return strings.ToLower(categoryID)
	}
}

// eonetSeverity turns a magnitude with a known unit into 0..1. EONET has no
// universal severity scale; an event without a magnitude stays at 0.
func eonetSeverity(categoryID string, value float64, unit string) float64 {
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "knots", "kt":
		return clamp01((value - 34) / 100)
	case "acres", "hectares", "ha":
		if value <= 0 {
			return 0
		}
		return clamp01(math.Log10(1+value) / 6)
	case "kilometers", "km":
		return clamp01(value / 100)
	case "feet", "ft":
		return clamp01(value / 100)
	default:
		_ = categoryID
		return 0
	}
}

// reliefWebDoc is the ReliefWeb API v2 report list.
type reliefWebDoc struct {
	Data []struct {
		ID     string `json:"id"`
		Fields struct {
			Title string `json:"title"`
			URL   string `json:"url"`
			Date  struct {
				Created  string `json:"created"`
				Changed  string `json:"changed"`
				Original string `json:"original"`
			} `json:"date"`
			Country []struct {
				Name     string `json:"name"`
				ISO3     string `json:"iso3"`
				Location struct {
					Lat float64 `json:"lat"`
					Lon float64 `json:"lon"`
				} `json:"location"`
			} `json:"country"`
			PrimaryCountry struct {
				Name     string `json:"name"`
				ISO3     string `json:"iso3"`
				Location struct {
					Lat float64 `json:"lat"`
					Lon float64 `json:"lon"`
				} `json:"location"`
			} `json:"primary_country"`
			Source []struct {
				ShortName string `json:"shortname"`
				Name      string `json:"name"`
			} `json:"source"`
			DisasterType []struct {
				Name string `json:"name"`
			} `json:"disaster_type"`
		} `json:"fields"`
	} `json:"data"`
}

// ParseReliefWeb decodes a ReliefWeb API v2 report list. Only metadata is
// read: title, link, date, country centroid and disaster type. The report body
// is never requested.
func ParseReliefWeb(r io.Reader) ([]HazardItem, error) {
	var doc reliefWebDoc
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("fetch: parse reliefweb json: %w", err)
	}
	out := make([]HazardItem, 0, len(doc.Data))
	for _, d := range doc.Data {
		f := d.Fields
		item := HazardItem{
			ID:        d.ID,
			Revision:  firstNonEmpty(f.Date.Changed, f.Date.Created),
			Title:     cleanText(f.Title, 300),
			URL:       f.URL,
			Precision: PrecisionCountry,
		}
		if item.URL == "" && d.ID != "" {
			item.URL = "https://reliefweb.int/node/" + d.ID
		}
		if item.URL == "" {
			continue
		}
		item.Published = firstTime(f.Date.Created, f.Date.Original)
		if len(f.DisasterType) > 0 {
			item.Hazard = strings.ToLower(strings.TrimSpace(f.DisasterType[0].Name))
			for _, dt := range f.DisasterType {
				item.Themes = append(item.Themes, "RELIEFWEB_"+strings.ToUpper(strings.ReplaceAll(dt.Name, " ", "_")))
			}
		}
		country := f.PrimaryCountry
		if country.Name == "" && len(f.Country) > 0 {
			country.Name, country.ISO3, country.Location = f.Country[0].Name, f.Country[0].ISO3, f.Country[0].Location
		}
		item.Country = country.ISO3
		item.Place = country.Name
		if validCoords(country.Location.Lat, country.Location.Lon) {
			item.Lat, item.Lon, item.Located = country.Location.Lat, country.Location.Lon, true
		}
		var summary []string
		if country.Name != "" {
			summary = append(summary, country.Name)
		}
		if len(f.Source) > 0 {
			summary = append(summary, firstNonEmpty(f.Source[0].ShortName, f.Source[0].Name))
		}
		for _, dt := range f.DisasterType {
			summary = append(summary, dt.Name)
		}
		item.Summary = truncateRunes(strings.Join(summary, " · "), 400)
		out = append(out, item)
	}
	return out, nil
}

// revisionString prefers the update time over the origin time.
func revisionString(updatedMS, originMS int64) string {
	if updatedMS != 0 {
		return strconv.FormatInt(updatedMS, 10)
	}
	if originMS != 0 {
		return strconv.FormatInt(originMS, 10)
	}
	return ""
}

func unixMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
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
