package geo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Photon is the optional open geocoder for street, landmark and point
// precision. It is not required: when BaseURL is empty or a request fails the
// plotter keeps the gazetteer candidate and marks precision honestly.
type Photon struct {
	base   string
	client *http.Client
}

// NewPhoton returns nil when base is empty, which disables the fallback.
func NewPhoton(base string) *Photon {
	base = strings.TrimSpace(strings.TrimRight(base, "/"))
	if base == "" {
		return nil
	}
	return &Photon{base: base, client: &http.Client{Timeout: 4 * time.Second}}
}

// Geocode returns up to limit Photon features for query, biased near lat/lon
// when the bias is valid.
func (p *Photon) Geocode(ctx context.Context, query string, lat, lon float64, limit int) ([]Place, error) {
	if p == nil {
		return nil, fmt.Errorf("geo: photon disabled")
	}
	if limit <= 0 || limit > 8 {
		limit = 1
	}
	q := url.Values{}
	q.Set("q", query)
	q.Set("limit", strconv.Itoa(limit))
	if ValidCoordinates(lat, lon) {
		q.Set("lat", fmt.Sprintf("%.5f", lat))
		q.Set("lon", fmt.Sprintf("%.5f", lon))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/api/?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "dor-geo/0.1 (+https://github.com/ankojh/dor)")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("geo: photon: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("geo: photon HTTP %d", resp.StatusCode)
	}
	var body struct {
		Features []struct {
			Geometry struct {
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
			Properties struct {
				OSMType     string `json:"osm_type"`
				OSMID       int64  `json:"osm_id"`
				Name        string `json:"name"`
				City        string `json:"city"`
				State       string `json:"state"`
				Country     string `json:"country"`
				CountryCode string `json:"countrycode"`
				Type        string `json:"type"`
			} `json:"properties"`
		} `json:"features"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("geo: photon decode: %w", err)
	}
	out := make([]Place, 0, len(body.Features))
	for _, f := range body.Features {
		if len(f.Geometry.Coordinates) < 2 || !ValidCoordinates(f.Geometry.Coordinates[1], f.Geometry.Coordinates[0]) {
			continue
		}
		name := f.Properties.Name
		if name == "" {
			name = f.Properties.City
		}
		if name == "" {
			continue
		}
		id := "photon:" + f.Properties.OSMType + ":" + strconv.FormatInt(f.Properties.OSMID, 10)
		out = append(out, Place{
			ID:          id,
			Name:        name,
			Normalized:  normalize(name),
			Admin:       firstNonEmpty(f.Properties.State, f.Properties.City),
			Country:     f.Properties.Country,
			CountryCode: strings.ToUpper(f.Properties.CountryCode),
			Lat:         f.Geometry.Coordinates[1],
			Lon:         f.Geometry.Coordinates[0],
			Feature:     f.Properties.Type,
			Score:       1,
		})
	}
	return out, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
