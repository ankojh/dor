# Adapter fixtures

Real responses captured 2026-11-20 with the production User-Agent
(`DorBot/0.1 (+https://github.com/ankojh/dor)`). Tests decode these with no
network. Large feeds and API responses are trimmed to the first few items; the
bytes of each kept item are untouched.

| File | Source | Notes |
| --- | --- | --- |
| `rss_bbc.xml` | `https://feeds.bbci.co.uk/news/world/rss.xml` | RSS 2.0, first 5 items |
| `atom_verge.xml` | `https://www.theverge.com/rss/index.xml` | Atom, first 5 entries |
| `rdf_dw.xml` | `https://rss.dw.com/rdf/rss-en-all` | RSS 1.0 / RDF, first 5 items |
| `rss_utf16_vietnam.xml` | `https://vietnamnews.vn/rss/home.rss` | UTF-16LE without a BOM |
| `rss_truncated.xml` | BBC world feed | cut mid-item, for the truncated-tail path |
| `not_a_feed.html` | `https://thewire.in/rss` | a 200 HTML app shell, not a feed |
| `gdelt_gkg.csv` | GDELT 2.0 `20261121050000.gkg.csv` | 4 real rows: allowlisted and not, themed and not, geocoded |
| `gdelt_export.csv` | GDELT 2.0 `20261121050000.export.CSV` | first 20 event rows |
| `usgs_earthquakes.json` | USGS `all_hour.geojson` | first 3 features |
| `gdacs_alerts.xml` | `https://www.gdacs.org/xml/rss_7d.xml` | first 3 items |
| `eonet_events.json` | NASA EONET v3 `events?status=open&days=7` | first 3 events |
| `reliefweb_403.json` | `https://api.reliefweb.int/v2/reports?...` | the real anonymous-access error |
| `reliefweb_reports.json` | ReliefWeb API v2 documentation shape | **synthetic**: anonymous access is 403, so the success path is covered by a documented-shape response |

The synthetic ReliefWeb response exists only to exercise the adapter's field
mapping; it is not claimed to be a real capture. Enabling the ReliefWeb source
requires an approved appname (see `docs/sources.md`).
