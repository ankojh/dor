package gateway

import (
	"net/http"
	"strconv"
	"strings"
)

type placeJSON struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Admin       string  `json:"admin"`
	Country     string  `json:"country"`
	CountryCode string  `json:"country_code"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	Precision   string  `json:"precision"`
	Population  int64   `json:"population"`
}

type placesJSON struct {
	Places    []placeJSON `json:"places"`
	Available bool        `json:"available"`
}

// handlePlaces serves the Cmd-K fly-to search from the already-loaded
// gazetteer. It never downloads data, touches story state or calls a model.
func (s *Server) handlePlaces(w http.ResponseWriter, r *http.Request) {
	limit := 8
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}
	out := placesJSON{Places: []placeJSON{}, Available: false}
	if s.cfg.Places == nil || !s.cfg.Places.Loaded() {
		writeAPI(w, http.StatusOK, out)
		return
	}
	for _, p := range s.cfg.Places.Search(r.URL.Query().Get("query"), limit) {
		precision := "city"
		if p.CountryID {
			precision = "country"
		}
		out.Places = append(out.Places, placeJSON{
			ID: p.ID, Name: p.Name, Admin: p.Admin, Country: p.Country,
			CountryCode: p.CountryCode, Lat: p.Lat, Lon: p.Lon,
			Precision: precision, Population: p.Population,
		})
	}
	out.Available = true
	writeAPI(w, http.StatusOK, out)
}
