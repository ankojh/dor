package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/ankojh/dor/internal/geo"
	"github.com/jackc/pgx/v5"
)

type LocationRepairBundle = geo.LocationRepairBundle

type ownerKey struct{ table, id string }

func key(row location) ownerKey            { return ownerKey{row.Table, row.Owner} }
func voteKey(id string, role int16) string { return fmt.Sprintf("%s/%d", id, role) }
func vote(row location) geo.LocationVote {
	v := geo.LocationVote{PlaceID: row.PlaceID, Name: row.Name, Country: row.Country, Lat: row.Lat, Lon: row.Lon, Precision: row.Precision, Role: row.Role}
	if row.Table == "article_locations" {
		v.Confidence = row.Weight
	} else {
		v.Weight = row.Weight
	}
	return v
}
func sortVotes(v []geo.LocationVote) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].PlaceID != v[j].PlaceID {
			return v[i].PlaceID < v[j].PlaceID
		}
		return v[i].Role < v[j].Role
	})
}

// Hash numeric values as strings so even an old NaN/Inf sentinel has an exact,
// stable before hash. After arrays themselves always contain valid coordinates.
func locationHash(votes []geo.LocationVote, primary string) string {
	type scalar struct{ ID, Name, Country, Lat, Lon, Precision, Role, Weight, Confidence string }
	copyVotes := append([]geo.LocationVote(nil), votes...)
	sortVotes(copyVotes)
	vals := make([]scalar, 0, len(copyVotes))
	for _, v := range copyVotes {
		vals = append(vals, scalar{v.PlaceID, v.Name, v.Country, strconv.FormatFloat(v.Lat, 'g', -1, 64), strconv.FormatFloat(v.Lon, 'g', -1, 64), strconv.Itoa(int(v.Precision)), strconv.Itoa(int(v.Role)), strconv.FormatFloat(float64(v.Weight), 'g', -1, 32), strconv.FormatFloat(float64(v.Confidence), 'g', -1, 32)})
	}
	data, _ := json.Marshal(struct {
		Primary   string
		Locations []scalar
	}{primary, vals})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func plannedRows(before []location, changes []change) map[ownerKey][]geo.LocationVote {
	rows := map[ownerKey]map[string]geo.LocationVote{}
	for _, row := range before {
		k := key(row)
		if rows[k] == nil {
			rows[k] = map[string]geo.LocationVote{}
		}
		rows[k][voteKey(row.PlaceID, row.Role)] = vote(row)
	}
	for _, c := range changes {
		delete(rows[key(c.row)], voteKey(c.row.PlaceID, c.row.Role))
	}
	for _, c := range changes {
		if c.decision.Replacement == nil {
			continue
		}
		p := c.decision.Replacement
		owner := key(c.row)
		duplicate := false
		for existingKey, existing := range rows[owner] {
			if existing.PlaceID == p.ID {
				if existing.Country == "" && p.CountryCode != "" {
					existing.Country = p.CountryCode
				}
				if existing.Role == c.row.Role {
					if owner.table == "story_locations" {
						existing.Weight = max(existing.Weight, c.row.Weight)
					} else {
						existing.Confidence = max(existing.Confidence, c.row.Weight)
					}
				}
				rows[owner][existingKey] = existing
				duplicate = true
			}
		}
		if duplicate {
			continue
		}
		r := c.row
		r.Country = replacementCountry(r, p)
		r.PlaceID, r.Lat, r.Lon, r.Precision = p.ID, p.Lat, p.Lon, int16(c.decision.Precision)
		if p.Name != "" {
			r.Name = p.Name
		}
		k := key(r)
		v := vote(r)
		old, ok := rows[k][voteKey(r.PlaceID, r.Role)]
		if ok {
			v.Weight = max(v.Weight, old.Weight)
			v.Confidence = max(v.Confidence, old.Confidence)
		}
		rows[k][voteKey(r.PlaceID, r.Role)] = v
	}
	out := map[ownerKey][]geo.LocationVote{}
	for k, values := range rows {
		out[k] = make([]geo.LocationVote, 0, len(values))
		for _, v := range values {
			out[k] = append(out[k], v)
		}
		sortVotes(out[k])
	}
	return out
}
func primaryVote(votes []geo.LocationVote, previous string) (uint32, string) {
	best := -1
	for i, v := range votes {
		if best < 0 || (v.PlaceID == previous && votes[best].PlaceID != previous) ||
			(v.PlaceID == previous) == (votes[best].PlaceID == previous) && (v.Weight > votes[best].Weight || v.Weight == votes[best].Weight && (v.Role < votes[best].Role || v.Role == votes[best].Role && v.PlaceID < votes[best].PlaceID)) {
			best = i
		}
	}
	if best < 0 {
		return 0, ""
	}
	return uint32(best), votes[best].PlaceID
}
func wireFromVotes(votes []geo.LocationVote, primary uint32) geo.EffectiveWireLocation {
	if len(votes) == 0 {
		return geo.EffectiveWireLocation{}
	}
	v := votes[primary]
	return geo.EffectiveWireLocation{Place: v.Name, Lat: v.Lat, Lon: v.Lon, Located: true}
}
func buildBundle(ctx context.Context, tx pgx.Tx, g *geo.Gazetteer, before []location, changes []change) (LocationRepairBundle, error) {
	after := plannedRows(before, changes)
	original := map[ownerKey][]geo.LocationVote{}
	for _, row := range before {
		original[key(row)] = append(original[key(row)], vote(row))
	}
	stories, articles := map[string]bool{}, map[string]bool{}
	for _, c := range changes {
		if c.row.Table == "story_locations" {
			stories[c.row.Owner] = true
		} else {
			articles[c.row.Owner] = true
		}
	}
	bundle := LocationRepairBundle{Version: 1, DatasetSHA256: g.DatasetFingerprint(), RevisionBinding: geo.RepairRevisionBinding, Stories: []geo.StoryLocationPatch{}, Articles: []geo.ArticleLocationPatch{}}
	rows, err := tx.Query(ctx, `SELECT id,primary_loc FROM public.stories ORDER BY id`)
	if err != nil {
		return bundle, err
	}
	eventCandidates := map[string]geo.EffectiveWireLocation{}
	for rows.Next() {
		var id, previous string
		if err := rows.Scan(&id, &previous); err != nil {
			rows.Close()
			return bundle, err
		}
		k := ownerKey{"story_locations", id}
		votes := after[k]
		idx, primary := primaryVote(votes, previous)
		eventCandidates[id] = wireFromVotes(votes, idx)
		if !stories[id] {
			continue
		}
		bundle.Stories = append(bundle.Stories, geo.StoryLocationPatch{ID: id, DBBeforeHash: locationHash(original[k], previous), BeforePrimaryPlaceID: previous, Locations: votes, Primary: idx, PrimaryPlaceID: primary, Unlocated: len(votes) == 0})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return bundle, err
	}

	rows, err = tx.Query(ctx, `SELECT DISTINCT id,COALESCE(story_id,'') FROM public.articles ORDER BY id,COALESCE(story_id,'')`)
	if err != nil {
		return bundle, err
	}
	articleStories := map[string]string{}
	for rows.Next() {
		var id, story string
		if err := rows.Scan(&id, &story); err != nil {
			rows.Close()
			return bundle, err
		}
		if old, ok := articleStories[id]; ok && old != story {
			rows.Close()
			return bundle, fmt.Errorf("article %s has inconsistent story membership", id)
		}
		articleStories[id] = story
		if stories[story] {
			articles[id] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return bundle, err
	}
	ids := make([]string, 0, len(articles))
	for id := range articles {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		k := ownerKey{"article_locations", id}
		votes := after[k]
		if votes == nil {
			votes = []geo.LocationVote{}
		}
		primary, _ := primaryVote(votes, "")
		wire := wireFromVotes(votes, primary)
		// This is only an event-location candidate, not a final wire patch.
		// Actual article hints/wire evidence must be bound by the hot planner.
		if event, ok := eventCandidates[articleStories[id]]; ok {
			wire = event
		}
		bundle.Articles = append(bundle.Articles, geo.ArticleLocationPatch{ID: id, StoryID: articleStories[id], DBBeforeHash: locationHash(original[k], ""), Locations: votes, Unlocated: len(votes) == 0, EventCandidate: wire, NeedsHotEvidence: true})
	}
	bundle.RunID, err = geo.LocationRepairBundleRunID(bundle)
	if err != nil {
		return bundle, err
	}
	if err := geo.ValidateLocationRepairBundle(bundle); err != nil {
		return bundle, err
	}
	return bundle, nil
}
func verifyBundle(ctx context.Context, tx pgx.Tx, bundle LocationRepairBundle) error {
	rows, err := loadLocations(ctx, tx)
	if err != nil {
		return err
	}
	byOwner := map[ownerKey][]geo.LocationVote{}
	for _, row := range rows {
		byOwner[key(row)] = append(byOwner[key(row)], vote(row))
	}
	for _, p := range bundle.Stories {
		var primary string
		if err := tx.QueryRow(ctx, `SELECT primary_loc FROM public.stories WHERE id=$1`, p.ID).Scan(&primary); err != nil {
			return err
		}
		if locationHash(byOwner[ownerKey{"story_locations", p.ID}], primary) != locationHash(p.Locations, p.PrimaryPlaceID) {
			return fmt.Errorf("story %s SQL does not match planned bundle", p.ID)
		}
	}
	for _, p := range bundle.Articles {
		if locationHash(byOwner[ownerKey{"article_locations", p.ID}], "") != locationHash(p.Locations, "") {
			return fmt.Errorf("article %s SQL does not match planned bundle", p.ID)
		}
	}
	return nil
}
